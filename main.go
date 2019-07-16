package main

import (
	"net/http"
	"encoding/json"
	"database/sql"
	_ "github.com/lib/pq"
	"strings"
	"log"
	"os"
)

type Company struct {
	CompanyID string `json:"comapnyID"`
	Countries []string `json:"countries"`
	Budget uint16 `json:"budget"`
	BudgetUnit string `json:"budgetUnit"`
	Bid uint8 `json:"bid"`
	BidUnit string `json:"bidUnit"`
	Version uint8 `json:"version"`
	Catergories []string `json:"categories"`
}

var db *sql.DB

func init() {
	var err error
	db, err = sql.Open("postgres", "host=localhost port=5432 user=postgres password=123 dbname=stocks_db sslmode=disable")
	if err != nil {
		panic(err)
	}
	if err = db.Ping(); err != nil {
		panic(err)
	}
}

func apiFunc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	f, err := os.OpenFile("log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
	    log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	if r.Method == "GET" {
		countrycode := r.FormValue("countrycode")
		category := r.FormValue("Category")
		basebid := r.FormValue("BaseBid")

		if countrycode == "" || category == "" || basebid == "" {
			http.Error(w, http.StatusText(400), 400)
			return
		}

		var cid string
		var count int

		rows, err := db.Query("SELECT CompanyID FROM stocks WHERE string_to_array(countries,',') && array[$1] AND string_to_array(category,',') && array[$2]", countrycode, category)
		if err != nil {
			log.Println(err)
			http.Error(w, http.StatusText(500), 500)
			return
		}

		count = 0
		for rows.Next() {
			err := rows.Scan(&cid)
			if err != nil {
				http.Error(w, http.StatusText(500), 500)
				return
			}
			log.Println(cid)
			count++
		}

		if count == 0 {
			log.Println("{C1, Failed},{C2,Failed},{C3,Failed}")
			w.Write([]byte("No Companies Passed from Targeting"))
			return
		}

		

		

		w.Write([]byte("C1"))
	}
}

// get db screenshot
func getAllData(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, http.StatusText(400), 400)
		return
	}

	var countries string
	var categories string

	rows, err := db.Query("SELECT CompanyID,Countries,Budget,bid,Category FROM stocks")
	if err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}

	defer rows.Close()
	company := make([]Company, 0)
	for rows.Next() {
		c := Company{}
		err := rows.Scan(&c.CompanyID, &countries, &c.Budget, &c.Bid, &categories)
		if err != nil {
			http.Error(w, http.StatusText(500), 500)
			return
		}
		c.Countries = strings.Split(countries, ",")
		c.Catergories = strings.Split(categories, ",")
		c.BidUnit = "cent"
		company = append(company, c)
	}

	if err = rows.Err(); err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(company)

}


func main() {
	http.HandleFunc("/", apiFunc)
	http.HandleFunc("/company", getAllData)
	http.ListenAndServe(":8080", nil)
}