package vo;

public class MemberStatData {

	private String linkID;
	private String REPORT_YMD;
	private int ADDRESS;
	private int GENDER;
	private String DEATH_YMD;
	private int DEATH_TIME;
	private int DEATH_PLACE;
	private String DEATH_JOB;
	private int MARRY;
	private int EDU;
	private String DEATH_CAU1;
	private String DEATH_CAU1_Parent;
	private int DEATH_AGE;
	
	public MemberStatData(String linkID, String REPORT_YMD, 
			int ADDRESS, int GENDER, String DEATH_YMD, int DEATH_TIME, int DEATH_PLACE, 
			String DEATH_JOB, int MARRY, int EDU ,String DEATH_CAU1, String DEATH_CAU1_Parent,
			int DEATH_AGE){
		
		this.linkID = linkID;
		this.REPORT_YMD = REPORT_YMD;
		this.ADDRESS = ADDRESS;
		this.GENDER = GENDER;
		this.DEATH_YMD = DEATH_YMD;
		this.DEATH_TIME = DEATH_TIME;
		this.DEATH_PLACE = DEATH_PLACE;
		this.DEATH_JOB = DEATH_JOB;
		this.MARRY = MARRY;
		this.EDU = EDU;
		this.DEATH_CAU1 = DEATH_CAU1;
		this.DEATH_CAU1_Parent = DEATH_CAU1_Parent;
		this.DEATH_AGE = DEATH_AGE;
	}

	public String getLinkID() {
		return linkID;
	}

	public void setLinkID(String linkID) {
		this.linkID = linkID;
	}

	public String getREPORT_YMD() {
		return REPORT_YMD;
	}

	public void setREPORT_YMD(String rEPORT_YMD) {
		REPORT_YMD = rEPORT_YMD;
	}

	public int getADDRESS() {
		return ADDRESS;
	}

	public void setADDRESS(int aDDRESS) {
		ADDRESS = aDDRESS;
	}

	public int getGENDER() {
		return GENDER;
	}

	public void setGENDER(int gENDER) {
		GENDER = gENDER;
	}

	public String getDEATH_YMD() {
		return DEATH_YMD;
	}

	public void setDEATH_YMD(String dEATH_YMD) {
		DEATH_YMD = dEATH_YMD;
	}

	public int getDEATH_TIME() {
		return DEATH_TIME;
	}

	public void setDEATH_TIME(int dEATH_TIME) {
		DEATH_TIME = dEATH_TIME;
	}

	public int getDEATH_PLACE() {
		return DEATH_PLACE;
	}

	public void setDEATH_PLACE(int dEATH_PLACE) {
		DEATH_PLACE = dEATH_PLACE;
	}

	public String getDEATH_JOB() {
		return DEATH_JOB;
	}

	public void setDEATH_JOB(String dEATH_JOB) {
		DEATH_JOB = dEATH_JOB;
	}

	public int getMARRY() {
		return MARRY;
	}

	public void setMARRY(int mARRY) {
		MARRY = mARRY;
	}

	public int getEDU() {
		return EDU;
	}

	public void setEDU(int eDU) {
		EDU = eDU;
	}

	public String getDEATH_CAU1() {
		return DEATH_CAU1;
	}

	public void setDEATH_CAU1(String dEATH_CAU1) {
		DEATH_CAU1 = dEATH_CAU1;
	}

	public String getDEATH_CAU1_Parent() {
		return DEATH_CAU1_Parent;
	}

	public void setDEATH_CAU1_Parent(String dEATH_CAU1_Parent) {
		DEATH_CAU1_Parent = dEATH_CAU1_Parent;
	}

	public int getDEATH_AGE() {
		return DEATH_AGE;
	}

	public void setDEATH_AGE(int dEATH_AGE) {
		DEATH_AGE = dEATH_AGE;
	}
	
	
	
}
