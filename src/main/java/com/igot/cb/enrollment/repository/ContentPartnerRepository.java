package com.igot.cb.enrollment.repository;

import com.igot.cb.enrollment.entity.ContentPartnerEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ContentPartnerRepository extends JpaRepository<ContentPartnerEntity,String> {
//    @Query(value = "SELECT * FROM content_partner WHERE data->>'contentPartnerName' = ?1 AND (data->>'isActive')::boolean = ?2", nativeQuery = true)
//    Optional<ContentPartnerEntity> getContentDetailsByContentPartnerNameAndIsActive(@Param("contentPartnerName") String name,@Param("isActive") boolean isActive);
//@Query(value = "SELECT * FROM content_partner WHERE data->>'contentPartnerName' = :contentPartnerName AND (data->>'isActive')::boolean = :isActive", nativeQuery = true)
//Optional<ContentPartnerEntity> findByContentPartnerNameAndIsActive(@Param("contentPartnerName") String contentPartnerName,@Param("isActive") boolean isActive);
@Query(value = "SELECT * FROM content_partner WHERE data->>'contentPartnerName' = :contentPartnerName", nativeQuery = true)
Optional<ContentPartnerEntity> findByContentPartnerName(@Param("contentPartnerName") String contentPartnerName);
}
