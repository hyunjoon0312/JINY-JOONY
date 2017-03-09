package vo;

public class MemberNhisData {

	private String linkID;
	private int STND_Y;
	private int YKIHO_ID;
	private int YKIHO_GUBUN_CD;
	private int ORG_TYPE;
	private int YKIHO_SIDO;
	private int SICKBED_CNT;
	private int DR_CNT;
	private int CT_YN;
	private int MRI_YN;
	private int PET_YN;
	
	public MemberNhisData(String linkID, int STND_Y, int YKIHO_ID, int YKIHO_GUBUN_CD, int ORG_TYPE, int YKIHO_SIDO, int SICKBED_CNT, int DR_CNT, int CT_YN, int MRI_YN, int PET_YN){
		
		this.linkID = linkID;
		this.STND_Y = STND_Y;
		this.YKIHO_ID = YKIHO_ID;
		this.YKIHO_GUBUN_CD = YKIHO_GUBUN_CD;
		this.ORG_TYPE = ORG_TYPE;
		this.YKIHO_SIDO = YKIHO_SIDO;
		this.SICKBED_CNT = SICKBED_CNT;
		this.DR_CNT = DR_CNT;
		this.CT_YN = CT_YN;
		this.MRI_YN = MRI_YN;
		this.PET_YN = PET_YN;
	}

	public String getLinkID() {
		return linkID;
	}

	public void setLinkID(String linkID) {
		this.linkID = linkID;
	}

	public int getSTND_Y() {
		return STND_Y;
	}

	public void setSTND_Y(int sTND_Y) {
		STND_Y = sTND_Y;
	}

	public int getYKIHO_ID() {
		return YKIHO_ID;
	}

	public void setYKIHO_ID(int yKIHO_ID) {
		YKIHO_ID = yKIHO_ID;
	}

	public int getYKIHO_GUBUN_CD() {
		return YKIHO_GUBUN_CD;
	}

	public void setYKIHO_GUBUN_CD(int yKIHO_GUBUN_CD) {
		YKIHO_GUBUN_CD = yKIHO_GUBUN_CD;
	}

	public int getORG_TYPE() {
		return ORG_TYPE;
	}

	public void setORG_TYPE(int oRG_TYPE) {
		ORG_TYPE = oRG_TYPE;
	}

	public int getYKIHO_SIDO() {
		return YKIHO_SIDO;
	}

	public void setYKIHO_SIDO(int yKIHO_SIDO) {
		YKIHO_SIDO = yKIHO_SIDO;
	}

	public int getSICKBED_CNT() {
		return SICKBED_CNT;
	}

	public void setSICKBED_CNT(int sICKBED_CNT) {
		SICKBED_CNT = sICKBED_CNT;
	}

	public int getDR_CNT() {
		return DR_CNT;
	}

	public void setDR_CNT(int dR_CNT) {
		DR_CNT = dR_CNT;
	}

	public int getCT_YN() {
		return CT_YN;
	}

	public void setCT_YN(int cT_YN) {
		CT_YN = cT_YN;
	}

	public int getMRI_YN() {
		return MRI_YN;
	}

	public void setMRI_YN(int mRI_YN) {
		MRI_YN = mRI_YN;
	}

	public int getPET_YN() {
		return PET_YN;
	}

	public void setPET_YN(int pET_YN) {
		PET_YN = pET_YN;
	}
	
	
	
}
