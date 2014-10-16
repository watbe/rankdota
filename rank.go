package main

import "fmt"
import "log"
import "math"

import (
        _ "github.com/lib/pq"
        "database/sql"
)

const defaultELO int = 1200
const weightingELO int = 50

func main() {
    dataset, err := sql.Open("postgres", "user=wayne dbname=trackdota_prod sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }

    store, err := sql.Open("postgres", "user=wayne dbname=rankdota sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }

    // start by resetting all elo
    _, err = store.Exec("UPDATE team SET elo = $1", defaultELO)
    if err != nil {
        log.Fatal(err)
    }

    rows, err := dataset.Query(`SELECT matches.id, matches.match_id, winner, winners.team_id, losers.team_id
        FROM 
            core_match AS matches 
        INNER JOIN 
            core_matchteam AS winners 
        ON matches.id = winners.match_id 
            AND matches.winner = winners.side 
        INNER JOIN
            core_matchteam AS losers
        ON matches.id = losers.match_id
            AND losers.team_id <> winners.team_id
        INNER JOIN 
            core_league AS league
        ON league.id = matches.league_id
    WHERE matches.match_id > 884352157 
        AND status >= 4
        AND league.tier = 3`)

    var i int = 0;
    var id, match_id, side, winner, loser int
    for rows.Next() {
        i = i + 1;
        if err := rows.Scan(&id, &match_id, &side, &winner, &loser); err != nil {
            log.Println(err)
        } else {
           // fmt.Printf("Match %d was won by %d against %d on side %d\n", match_id, winner, loser, side)
            winner_elo := getOrCreateTeamELO(store, dataset, winner)
            loser_elo := getOrCreateTeamELO(store, dataset, loser)
           // fmt.Printf("Before:: Winner ELO: %d | Loser ELO: %d\n", winner_elo, loser_elo)
            new_winner_elo, new_loser_elo := calculateELO(winner_elo, loser_elo)
            updateTeam(store, winner, new_winner_elo)
            updateTeam(store, loser, new_loser_elo)
           // fmt.Printf("After:: Winner ELO: %d | Loser ELO: %d\n", new_winner_elo, new_loser_elo)
        }
        if i % 100 == 0 {
            fmt.Printf("Processed %d matches\n", i)
        }
    }

    fmt.Println(match_id)

    fmt.Printf("\n\nResults:\n")

    rows, err = store.Query("SELECT name, elo FROM team ORDER BY elo DESC LIMIT 50")
    for rows.Next() {
        var name string
        var elo int
        if err := rows.Scan(&name, &elo); err != nil {
            log.Println(err)
        } else {
            fmt.Printf("%s: %d\n", name, elo)
        }
    }
}

func getOrCreateTeamELO(store *sql.DB, dataset *sql.DB, id int) int {
    var elo, count int
    err := store.QueryRow("SELECT elo, games_played FROM team WHERE id = $1", id).Scan(&elo, &count)

    switch {
        case err == sql.ErrNoRows:
           // log.Printf("Team %d not found in store, creating new entry with default ELO (%d)", id, defaultELO)
            _, name, err := getTeamDetails(dataset, id)
            if err != nil {
                log.Fatal(err)
            }
            elo = defaultELO
            count = 0
            createTeam(store, id, name, elo, count)
        case err != nil:
            log.Fatal(err)
    }

    return elo
}

// a is winner, b is loser
func calculateELO(a int, b int) (int, int) {
    var expected_a float64
    var new_a int
    expected_a = 1.0 / (1.0 + math.Pow(10, float64(a - b)/400.0))
    new_a = a + int(Round(float64(weightingELO) * (1 - expected_a), 0.5, 0))

    // ELO is symmetric so new b is the opposite in change
    new_b := b + a - new_a
    return new_a, new_b
}

func updateTeam(store *sql.DB, id int, elo int) {
    _, err := store.Exec("UPDATE team SET elo = $1 WHERE id = $2", elo, id)
    if err != nil {
        log.Fatal(err)
    }
}

func createTeam(store *sql.DB, id int, name string, elo int, count int) {
    _, err := store.Exec("INSERT INTO team VALUES ($1, $2, $3, $4)", id, name, elo, count)
    if err != nil {
        log.Fatal(err)
    }
}

func getTeamDetails(dataset *sql.DB, id int) (int, string, error) {
    // Look for the team in the dataset to get names
    var name sql.NullString
    err := dataset.QueryRow("SELECT name FROM core_team WHERE id = $1", id).Scan(&name)

    switch {
        case err == sql.ErrNoRows:
 //           log.Printf("No team with ID %d in dataset\n", id)
        case err != nil:
            log.Fatal(err)
        default:
   //         fmt.Printf("Fetched Team ID %d: %s\n", id, name)
    }

    var s string

    if name.Valid {
        s = name.String
    } else {
        s = ""
    }

    return id, s, err

}

// thanks to https://gist.github.com/DavidVaini/10308388
func Round(val float64, roundOn float64, places int ) (newVal float64) {
    var round float64
    pow := math.Pow(10, float64(places))
    digit := pow * val
    _, div := math.Modf(digit)
    if div >= roundOn {
        round = math.Ceil(digit)
    } else {
        round = math.Floor(digit)
    }
    newVal = round / pow
    return
}
