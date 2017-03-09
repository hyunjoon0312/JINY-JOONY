CREATE TABLE UploadFileInfo (
	uploadtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	subject VARCHAR(255) NOT NULL,
	filename VARCHAR(20) NOT NULL,
	filepath VARCHAR(255) NOT NULL,
	filesize INT(255) NOT NULL,
	uploaderid VARCHAR(20) NOT NULL,
	uploadername VARCHAR(20) NOT NULL,
	PRIMARY KEY (uploadtime)
);
	
	