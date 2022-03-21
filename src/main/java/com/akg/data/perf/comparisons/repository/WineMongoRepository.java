package com.akg.data.perf.comparisons.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.akg.data.perf.comparisons.entity.Wine;

public interface WineMongoRepository extends MongoRepository<Wine, String> {

	public List<Wine> findByCountry(String name);
}
