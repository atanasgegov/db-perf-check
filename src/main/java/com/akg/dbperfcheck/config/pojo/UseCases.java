package com.akg.dbperfcheck.config.pojo;

import java.util.List;

import lombok.Data;

@Data
public class UseCases {

	private Type activeUseCase = Type.ONE;
	private Execution one;
	private List<Execution> crud;
	
	public enum Type {

		ONE("one"),
	    CRUD("crud"); 

		private final String name;
	    private Type(String name) {
	        this.name = name;
	    }

	    public String getValue() {
			return name;
		}
	}

}
