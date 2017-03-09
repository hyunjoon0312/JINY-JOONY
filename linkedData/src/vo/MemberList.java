package vo;

import java.sql.Timestamp;

public class MemberList {

	private Timestamp uploadtime;
	private String subject;
	private String filename;
	private String uploaderid;
	private String uploadername;
	private int nhis;
	private int stat;
	private int nhis_send;
	private int stat_send;
	
	public MemberList(Timestamp uploadtime, String subject, String filename, String uploaderid, String uploadername, int nhis, int stat){
		this.uploadtime = uploadtime;
		this.filename = filename;
		this.uploaderid = uploaderid;
		this.uploadername = uploadername;
		this.nhis = nhis;
		this.stat = stat;
		this.subject = subject;
	}

	public int getNhis_send() {
		return nhis_send;
	}

	public void setNhis_send(int nhis_send) {
		this.nhis_send = nhis_send;
	}

	public int getStat_send() {
		return stat_send;
	}

	public void setStat_send(int stat_send) {
		this.stat_send = stat_send;
	}

	public Timestamp getUploadtime() {
		return uploadtime;
	}
	public void setUploadtime(Timestamp uploadtime) {
		this.uploadtime = uploadtime;
	}
	public String getSubject() {
		return subject;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getUploaderid() {
		return uploaderid;
	}
	public void setUploaderid(String uploaderid) {
		this.uploaderid = uploaderid;
	}
	public String getUploadername() {
		return uploadername;
	}
	public void setUploadername(String uploadername) {
		this.uploadername = uploadername;
	}
	public int getNhis() {
		return nhis;
	}
	public void setNhis(int nhis) {
		this.nhis = nhis;
	}
	public int getStat() {
		return stat;
	}
	public void setStat(int stat) {
		this.stat = stat;
	}
	
	
}
