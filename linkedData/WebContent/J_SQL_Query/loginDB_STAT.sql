CREATE TABLE STATusers(
	STATid VARCHAR(12) NOT NULL,
	STATpasswd VARCHAR(255) NOT NULL,
	STATname VARCHAR(20) NOT NULL,
	STATemail VARCHAR(30) NOT NULL,
	PRIMARY KEY(STATid)
	);
	
	INSERT INTO STATusers VALUES ('stattest', password('stattest'), 'joon', 'joon@gmail.com');