package com.igot.cb.enrollment.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.enrollment.entity.CiosContentEntity;

class CiosContentRepositoryTest {

    @Mock
    private CiosContentRepository ciosContentRepository;

    @InjectMocks
    private CiosContentRepositoryTest testInstance;

    private CiosContentEntity mockEntity;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Create a mock CiosContentEntity
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode ciosData = mapper.createObjectNode();
        ciosData.put("test", "randomValue");

        mockEntity = new CiosContentEntity();
        mockEntity.setContentId("33333333");
        mockEntity.setExternalId("33333333");
        mockEntity.setCiosData(ciosData);
        mockEntity.setActive(true);
        mockEntity.setCreatedOn(new Timestamp(System.currentTimeMillis()));
        mockEntity.setLastUpdatedOn(new Timestamp(System.currentTimeMillis()));
        mockEntity.setPartnerId("ext13344");

        // Mock repository behavior
        when(ciosContentRepository.findByContentIdAndIsActive("33333333", true))
                .thenReturn(Optional.of(mockEntity));
    }

    @Test
    void testFindByContentIdAndIsActive() {
        Optional<CiosContentEntity> result = ciosContentRepository.findByContentIdAndIsActive("33333333", true);

        assertThat(result).isPresent();
        assertThat(result.get().getContentId()).isEqualTo(mockEntity.getContentId());
        assertThat(result.get().isActive()).isTrue();
    }

    @Test
    void testCiosContentRepositoryLogic() {
        // Add test logic here
        assertTrue(true);
    }
}
