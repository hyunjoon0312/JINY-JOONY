package vo;

public class MemberKEY_STAT {
	
	private String personID;
	private String statID;
	
	public MemberKEY_STAT(String personID, String statID){
		this.personID = personID;
		this.statID = statID;
	}
	
	
	public String getpersonID() {
		return personID;
	}
	public void setpersonID(String personID) {
		this.personID = personID;
	}
	public String getstatID() {
		return statID;
	}
	public void setstatID(String statID) {
		this.statID = statID;
	}

	
	
}
