package vo;

public class MemberKEY_NHIS {
	
	private String personID;
	private String nhisID;
	
	public MemberKEY_NHIS(String personID, String nhisID){
		this.personID = personID;
		this.nhisID = nhisID;
	}
	
	
	public String getpersonID() {
		return personID;
	}
	public void setpersonID(String personID) {
		this.personID = personID;
	}
	public String getnhisID() {
		return nhisID;
	}
	public void setnhisID(String nhisID) {
		this.nhisID = nhisID;
	}

	
	
}
