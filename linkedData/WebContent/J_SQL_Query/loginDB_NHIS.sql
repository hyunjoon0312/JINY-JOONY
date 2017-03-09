CREATE TABLE NHISusers(
	NHISid VARCHAR(12) NOT NULL,
	NHISpasswd VARCHAR(255) NOT NULL,
	NHISname VARCHAR(20) NOT NULL,
	NHISemail VARCHAR(30) NOT NULL,
	PRIMARY KEY(NHISid)
	);
	
	INSERT INTO NHISusers VALUES ('nhistest', password('nhistest'), 'joon', 'joon@gmail.com');