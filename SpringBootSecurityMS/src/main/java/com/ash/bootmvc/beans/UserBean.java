package com.ash.bootmvc.beans;

import javax.persistence.Entity;
import javax.persistence.Table;

//@Entity
//@Table(name = "users")
public class UserBean {

	private String testName;
	private String userName;
	private String password;
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	
}
