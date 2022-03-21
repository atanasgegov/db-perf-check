package com.akg.data.perf.comparisons.dto;

import lombok.Data;

@Data
public class WineDTO {

	private int id;
	private String country;
	private String description;
	private String designation;
	private int points;
	private float price;
	private String province;
	private String region1;
	private String region2;
	private String variety;
	private String winery;
}
