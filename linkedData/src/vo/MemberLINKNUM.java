package vo;

public class MemberLINKNUM {

	private int linkID;
	private String nhisID;
	private String statID;
	
	
	public MemberLINKNUM(int linkID, String nhisID, String statID){
		this.linkID = linkID;
		this.nhisID = nhisID;
		this.statID = statID;
	}
	
	
	public int getLinkID() {
		return linkID;
	}
	public void setLinkID(int linkID) {
		this.linkID = linkID;
	}
	public String getNhisID() {
		return nhisID;
	}
	public void setNhisID(String nhisID) {
		this.nhisID = nhisID;
	}
	public String getStatID() {
		return statID;
	}
	public void setStatID(String statID) {
		this.statID = statID;
	}
	
	
	
	
}
