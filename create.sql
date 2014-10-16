CREATE TABLE team (
    id              integer NOT NULL PRIMARY KEY,
    name            varchar(256) NOT NULL,
    elo             integer NOT NULL,
    games_played    integer NOT NULL
);
