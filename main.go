package main

import (
	"net/http"
	"encoding/json"
	"database/sql"
	_ "github.com/lib/pq"
	"strings"
	"log"
	"os"
	"strconv"
	"fmt"
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
	var winner string
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
		
		passed_companies := doBaseTargeting(countrycode, category);
		if len(passed_companies) == 0 {
			log.Println("{C1, Failed},{C2, Failed},{C3, Failed}")
			w.Write([]byte("No Companies Passed from Targeting"))
			return
		}

		log.Println("BaseTargeting: " + createLog(passed_companies))

		budget_passed_companies := doBudgetCheck(passed_companies)
		if len(budget_passed_companies) == 0 {
			log.Println("{C1, Failed},{C2, Failed},{C3, Failed}")
			w.Write([]byte("No Companies Passed from Budget"))
			return
		}
		log.Println("BudgetCheck: " + createLog(budget_passed_companies))

		bid_passed_companies := doBidCheck(budget_passed_companies, basebid)
		if len(bid_passed_companies) == 0 {
			log.Println("{C1, Failed},{C2, Failed},{C3, Failed}")
			w.Write([]byte("No Companies Passed from BaseBid Check"))
			return
		}
		log.Println("BaseBid: " + createLog(bid_passed_companies))
		
		if len(bid_passed_companies) == 1 {
			winner = bid_passed_companies[0]
			log.Println("Winner: " + winner)
		} else {
			winner = chooseWinner(bid_passed_companies)
		}

		w.Write([]byte(winner))


		reduceBudget(winner, basebid)
	}
}

// base targeting
func doBaseTargeting(countrycode string, category string) []string {
	passed_companies := make([]string, 0)
	var cid string
	rows, err := db.Query("SELECT CompanyID FROM stocks WHERE string_to_array(countries,',') && array[$1] AND string_to_array(category,',') && array[$2]", countrycode, category)
	if err == nil {
		for rows.Next() {
			err := rows.Scan(&cid)
			if err == nil {
				passed_companies = append(passed_companies, cid)
			}
		}
	}
	return passed_companies
}

// budget checking
func doBudgetCheck(passed_companies []string) []string {
	var diff int
	budget_passed_companies := make([]string, 0)
	for _, c := range passed_companies {
        row := db.QueryRow("SELECT budget-bid FROM stocks WHERE CompanyID=$1", c)
        err := row.Scan(&diff)
        if err == nil && diff >= 0 {
        	budget_passed_companies = append(budget_passed_companies, c)
        }
    }
    return budget_passed_companies
}

// base bid check
func doBidCheck(passed_companies []string, basebid string) []string {
	bid_passed_companies := make([]string, 0)
	var company_bid int
	apibid, err := strconv.Atoi(basebid)
	if err == nil {
		for _, c := range passed_companies {
	        row := db.QueryRow("SELECT bid FROM stocks WHERE CompanyID=$1", c)
	        err := row.Scan(&company_bid)
	        if err == nil && company_bid > apibid {
	        	bid_passed_companies = append(bid_passed_companies, c)
	        }
	    }
	}
    return bid_passed_companies
}

// choose winner
func chooseWinner(passed_companies []string) string {
	var winner string
	sql := fmt.Sprintf(`SELECT companyid FROM stocks WHERE companyid IN('%s') ORDER BY budget DESC LIMIT 1`, strings.Join(passed_companies, "','"))
    row := db.QueryRow(sql)
    err := row.Scan(&winner)
    if err != nil {
    	panic(err)
    }
    return winner
}


// reduce budget
func reduceBudget(winner string, basebid string) {
	bid, er := strconv.Atoi(basebid)
	if er == nil {
		_, err := db.Exec("UPDATE stocks SET budget=budget-$1 WHERE companyid=$2", bid, winner)
		if err != nil {
			panic(err)
		}
	}
} 

// create log
func createLog(passed_companies []string) string {
	log_string := "";
	var all_companies = [3]string{"C1", "C2", "C3"}

	for _, c := range all_companies {
		if contains(passed_companies, c) {
			log_string += "{" + c +", Passed},"
		} else {
			log_string += "{" + c +", Failed},"
		}
    }
	return log_string[0:len(log_string) - 1]
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
		c.BudgetUnit = "cent"
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

func contains(s []string, c string) bool {
    for _, a := range s {
        if a == c {
            return true
        }
    }
    return false
}


func main() {
	http.HandleFunc("/", apiFunc)
	http.HandleFunc("/company", getAllData)
	http.ListenAndServe(":8080", nil)
}