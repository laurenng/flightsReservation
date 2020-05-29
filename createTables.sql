


CREATE TABLE Users (
    Userid VARCHAR(20) PRIMARY KEY,
    salt varbinary(20),
    password varbinary(20),
    balance INT
);


CREATE TABLE Itineraries (
    searchID VARCHAR(50),
    id INT,
    origin VARCHAR(50),
    destination VARCHAR(50),
    stopover VARCHAR(50),
    fidFirst INT REFERENCES Flights,  -- flight number for direct flight
    fidSecond INT, --REFERENCES Flights, -- flight number for indirect flight
    flightTime INT,
    PRIMARY KEY (searchID, id)
);

SElect * from Itineraries;

CREATE TABLE Reservations (
    rid INT PRIMARY KEY,
    username VARCHAR(20) REFERENCES Users,
    day INT,
    flight1 INT REFERENCES Flights, -- fid number
    flight2 INT ,     -- fid number
    paid INT,  -- 0 = no, 1 = yes
    cancelled INT -- 0 = no, 1 = yes
);