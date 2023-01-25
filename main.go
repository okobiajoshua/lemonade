package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

var mu sync.Mutex
var db map[int]User
var verificationQueue chan User
var transactionQueue chan Transaction

func init() {
	db = make(map[int]User)
	verificationQueue = make(chan User, 1000)
	transactionQueue = make(chan Transaction, 1000)
}

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/user", CreateUser).Methods("POST")
	r.HandleFunc("/user", GetUser).Methods("GET")
	r.HandleFunc("/transaction", Transfer).Methods("POST")

	srv := &http.Server{
		Handler: r,
		Addr:    "127.0.0.1:8000",
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	// Note: x=2 used here. Running 2 worker go routines per time
	go processVerificationQueue(2, verifyUser)
	go processTransactionQueue(2, processTransaction)
	log.Fatal(srv.ListenAndServe())
}

type User struct {
	ID       int     `json:"id"`
	Balance  float64 `json:"balance"`
	Verified bool    `json:"verified"`
}

type Transaction struct {
	SenderID   int     `json:"sender_id" binding:"required"`
	ReceiverID int     `json:"receiver_id" binding:"required"`
	Amount     float64 `json:"amount" binding:"required"`
}

func GetUser(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(db)
}

func CreateUser(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Accept", "application/json")
	w.WriteHeader(200)
	var user User
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		http.Error(w, "Bad request", 400)
		return
	}
	fmt.Println("User: ", user)
	user, err = addUser(user)
	if err != nil {
		http.Error(w, "Error occured. Try again later", 500)
		return
	}
	fmt.Println("DB: ", db)
	addToVerificationQueue(user)
	err = json.NewEncoder(w).Encode(user)
	if err != nil {
		http.Error(w, "Error occured. Try again later", 500)
		return
	}
}

func addUser(user User) (User, error) {
	id := len(db) + 1
	user.ID = id
	user.Balance = float64(1000)
	db[id] = user
	return user, nil
}

func addToVerificationQueue(user User) error {
	verificationQueue <- user
	return nil
}

func verifyUser(user User) error {
	user.Verified = true
	db[user.ID] = user
	return nil
}

func processVerificationQueue(x int, f func(User) error) {
	// Every 30 seconds
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan bool)
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if len(verificationQueue) > 0 { // Added check to avoid spinning too many idle goroutines
				for i := 0; i < x; i++ {
					fmt.Println("new goroutine")
					go f(<-verificationQueue)
				}
			}
		}
	}
}

func processTransactionQueue(x int, f func(Transaction) error) {
	// Every 30 seconds
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan bool)
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if len(transactionQueue) > 0 { // Added check to avoid spinning too many idle goroutines
				for i := 0; i < x; i++ {
					go f(<-transactionQueue)
				}
			}
		}
	}
}

func processTransaction(t Transaction) error {
	user, ok := db[t.SenderID]
	if !ok {
		return errors.New("user not found")
	}
	if !user.Verified {
		verificationQueue <- user
		transactionQueue <- t
		return nil
	}

	mu.Lock()
	defer mu.Unlock()
	if user.Balance >= t.Amount {
		user.Balance -= t.Amount
		db[user.ID] = user

		rec := db[t.ReceiverID]
		rec.Balance += t.Amount
		db[rec.ID] = rec
	}

	return nil
}

func Transfer(w http.ResponseWriter, r *http.Request) {
	var t Transaction
	err := json.NewDecoder(r.Body).Decode(&t)
	if err != nil {
		http.Error(w, "Bad request", 400)
		return
	}

	transactionQueue <- t
	w.Write([]byte("Transaction is being processed"))
}
