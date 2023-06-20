package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const Version = "1.0.0"
const RIGHTS = 0755
const EXTENSION = ".json"
const TMP = ".tmp"
const USERS = "users"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger
}

func New(dir string, options *Options) (*Driver, error) {
	// Clean - нормализует путь, очищает от посторонних символов и "подгоняет" под нужную ОС
	dir = filepath.Clean(dir)

	opts := Options{}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	// Stat -  returns a FileInfo describing the named file.
	if _, err := os.Stat(dir); err != nil {
		opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'...\n", dir)
	return &driver, os.MkdirAll(dir, RIGHTS)
}

func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("[WRITE] missing collection")
	}
	if resource == "" {
		return fmt.Errorf("[WRITE] missing resource")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+EXTENSION)
	tmpPath := fnlPath + TMP

	if err := os.MkdirAll(dir, RIGHTS); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	b = append(b, byte('\n'))

	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}
func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("[READ] missing collection")
	}
	if resource == "" {
		return fmt.Errorf("[READ] missing resource")
	}

	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil {
		return err
	}

	bytes, err := os.ReadFile(record + EXTENSION)
	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, &v)
}
func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("[READ_ALL] missing collection")
	}

	dir := filepath.Join(d.dir, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}
	files, _ := os.ReadDir(dir)

	var records []string

	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) Delete(collection, resource string) error {
	if collection == "" {
		return fmt.Errorf("[DELETE] missing collection")
	}
	if resource == "" {
		return fmt.Errorf("[DELETE] missing resource")
	}

	path := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	// Mode() - используется для получения режима доступа (permissions) файла или директории.
	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("[DELETE] unable to find directory")
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + EXTENSION)
	}
	return nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	// Заблокировать mutex на случай, если он существует
	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")

		if err != nil {
			return nil, err
		}

		return fi, nil
	}

	return
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {
	dir := "./"

	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Error", err)
	}

	for _, value := range employees {
		err := db.Write(USERS, value.Name, User{
			Name:    value.Name,
			Age:     value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
		if err != nil {
			log.Fatal("[MAIN] write error", err)
		}
	}

	records, err := db.ReadAll(USERS)

	if err != nil {
		fmt.Println("Error [RECORDS]:", err)
	}
	fmt.Println("Records: ", records)

	var allUsers []User

	for _, f := range records {
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
			fmt.Println("Error [UNMARSHAL]", err)
		}
		allUsers = append(allUsers, employeeFound)
	}

	fmt.Println("Users: ", allUsers)

	var myUser User
	err = db.Read(USERS, "Zhora", &myUser)
	if err != nil {
		fmt.Println("Error [READ]", err)
	}

	fmt.Println("User: ", myUser)

	if err := db.Delete(USERS, "John"); err != nil {
		fmt.Println("Error [DELETE]", err)
	}

	if err := db.Delete(USERS, ""); err != nil {
		fmt.Println("Error [DELETE] not existing", err)
	}
}
