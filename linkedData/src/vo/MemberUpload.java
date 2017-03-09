package vo;

import java.sql.Timestamp;

public class MemberUpload {
	
	private Timestamp uploadtime;
	private String filename;
	private int	filesize;
	private String uploaderid;
	private String uploadername;
	
	
	public Timestamp getUploadtime(){
		return uploadtime;
	}
	
	public void setUploadtime(Timestamp uploadtime) {
		this.uploadtime = uploadtime;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public int getFilesize() {
		return filesize;
	}
	public void setFilesize(int filesize) {
		this.filesize = filesize;
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

	
}
