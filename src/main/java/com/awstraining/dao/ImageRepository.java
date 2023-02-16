package com.awstraining.dao;

import com.awstraining.entity.ImageModel;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ImageRepository extends JpaRepository<ImageModel, Long> {

    Optional<ImageModel> getOneByName(String name);

    List<ImageModel> findAllByName(String name);

    @Query(nativeQuery = true, value = "SELECT * FROM image ORDER BY RAND() limit 1;")
    ImageModel getOneRandom();

    void deleteByName(String name);
}
