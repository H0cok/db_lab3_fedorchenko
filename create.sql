CREATE TABLE Vegetables (
    vegetable_id int  NOT NULL PRIMARY KEY,
    vegetable_name char(256)  NOT NULL
);


CREATE TABLE Conditions (
	condition_id int  NOT NULL PRIMARY KEY,
	condition_name char(256)  NOT NULL
);


CREATE table purchase(
    purch_id int primary key not null,
    price int,
    temperature int
);

CREATE TABLE Purch_veg (
    purch_id int NOT NULL PRIMARY KEY,
    vegetable_id int NOT NULL,
    CONSTRAINT FK_vegetable_id FOREIGN KEY (vegetable_id) 
        REFERENCES Vegetables(vegetable_id),
    CONSTRAINT FK_purch_id FOREIGN KEY (purch_id) 
        REFERENCES Purchase(purch_ID)
);



CREATE TABLE Purch_cond (
purch_id int NOT NULL PRIMARY KEY,
condition_id int NOT NULL,
CONSTRAINT FK_condition_id FOREIGN KEY (condition_id) 
    REFERENCES Conditions(condition_id),
CONSTRAINT FK_purch_id FOREIGN KEY (purch_id) 
    REFERENCES Purchase(purch_id)
);


