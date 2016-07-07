package datatype;

import java.text.SimpleDateFormat;
import java.util.Date;


public class Meta implements Comparable<Meta> {
	public long time;
	public long userid;
	public int state;
	public double longitude;
	public double latitude;
	public int cluNo;

	public int site;

	private SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss.ss");

	public Meta(Meta m) {
		this.time = m.time;
		this.userid = m.userid;
		this.state = m.state;
		this.longitude = m.longitude;
		this.latitude = m.latitude;
		this.cluNo = m.cluNo;
		this.site = m.site;
	}

	public Meta(long time, long userid, int state, double lac, double cell, int cluNo) {
		this.time = time;
		this.userid = userid;
		this.state = state;
		this.longitude = lac;
		this.latitude = cell;
		this.cluNo = cluNo;
	}

	public Meta(long time, long userid, int state, double lac, double cell) {
		this.time = time;
		this.userid = userid;
		this.state = state;
		this.longitude = lac;
		this.latitude = cell;
	}

	@Override
	public int compareTo(Meta o) {
		// TODO Auto-generated method stub
		int res = 0;
		if (this.time > o.time) {
			res = 1;
		} else if (this.time == o.time) {
			res = 0;
		} else {
			res = -1;
		}
		return res;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		boolean res = false;
		if (obj instanceof Meta) {
			Meta o = (Meta) obj;
			res = true;
			if (this.time != o.time || this.userid != o.userid
					|| this.state != o.state || this.longitude != o.longitude
					|| this.latitude != o.latitude) {
				res = false;
			}
		}
		return res;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return sdf.format(new Date(time));
	}
	
	public Meta clone() {
		Meta ret = new Meta(this.time, this.userid, this.state, this.longitude, this.latitude, this.cluNo);
		ret.site = this.site;
		return ret;
	}

}