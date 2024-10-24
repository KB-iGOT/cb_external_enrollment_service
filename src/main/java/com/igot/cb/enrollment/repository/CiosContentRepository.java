package com.igot.cb.enrollment.repository;


import com.igot.cb.enrollment.entity.CiosContentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CiosContentRepository extends JpaRepository<CiosContentEntity,String> {

    @Query("SELECT c FROM CiosContentEntity c WHERE c.contentId = :contentId AND c.isActive = :isActive")
    Optional<CiosContentEntity> findByContentIdAndIsActive(@Param("contentId") String contentId, @Param("isActive") boolean isActive);
}
