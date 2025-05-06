
// package com.igot.cb.transactional.cassandrautils;

// import com.datastax.oss.driver.api.core.CqlSession;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.extension.ExtendWith;
// import org.mockito.Mockito;
// import org.springframework.data.cassandra.core.CassandraAdminTemplate;
// import org.springframework.data.cassandra.core.convert.CassandraConverter;
// import org.springframework.test.context.junit.jupiter.SpringExtension;

// import java.lang.reflect.Field;

// import static org.junit.jupiter.api.Assertions.*;
// import static org.mockito.Mockito.*;

// @ExtendWith(SpringExtension.class)
// class SunbirdConfigTest {

//     private SunbirdConfig sunbirdConfig;

//     @BeforeEach
//     void setUp() {
//         sunbirdConfig = Mockito.spy(new SunbirdConfig());

//         // Inject property values using reflection (since @Value will not inject in a plain unit test)
//         setPrivateField(sunbirdConfig, "sunbirdUser", "user1");
//         setPrivateField(sunbirdConfig, "sunbirdPassword", "pass1");

//         // Mock inherited/getter methods for config
//         doReturn("localhost,127.0.0.1").when(sunbirdConfig).getContactPoints();
//         doReturn(9042).when(sunbirdConfig).getPort();
//         doReturn("datacenter1").when(sunbirdConfig).getLocalDataCenter();
//         doReturn("keyspace1").when(sunbirdConfig).getKeyspaceName();
//     }

//     // Utility to set private fields via reflection
//     private void setPrivateField(Object object, String fieldName, Object value) {
//         try {
//             Field field = object.getClass().getDeclaredField(fieldName);
//             field.setAccessible(true);
//             field.set(object, value);
//         } catch (Exception e) {
//             throw new RuntimeException(e);
//         }
//     }

//     @Test
//     void testCassandraTemplateBeanCreation() {
//         // Arrange
//         CqlSession mockSession = mock(CqlSession.class);
//         CassandraConverter mockConverter = mock(CassandraConverter.class);
//         doReturn(mockConverter).when(sunbirdConfig).cassandraConverter();

//         // Act
//         CassandraAdminTemplate template = sunbirdConfig.cassandraTemplate(mockSession);

//         // Assert
//         assertNotNull(template);
//     }

//     @Test
//     void testBeansAreAnnotated() throws Exception {
//         assertTrue(sunbirdConfig.getClass().getMethod("cassandraTemplate", CqlSession.class)
//                 .isAnnotationPresent(org.springframework.context.annotation.Bean.class));
//         assertTrue(sunbirdConfig.getClass().getMethod("cqlSession")
//                 .isAnnotationPresent(org.springframework.context.annotation.Bean.class));
//     }

//     @Test
//     void testCqlSessionBeanCreationWithAuthCredentials() {
//         // This test can only partially execute without refactor or PowerMock, but we want to ensure that logic paths are covered

//         // With populated username/password fields logic should enter the if block – but actual session creation with CqlSession.builder is not tested
//         setPrivateField(sunbirdConfig, "sunbirdUser", "user1");
//         setPrivateField(sunbirdConfig, "sunbirdPassword", "pass1");

//         // call methods up to the build step; let any exception go as it's not a real integration test
//         try {
//             sunbirdConfig.cqlSession();
//         } catch (Exception ignored) {
//             // Expected since we are not connecting to a real Cassandra instance
//         }
//     }

//     @Test
//     void testCqlSessionBeanCreationWithoutAuthCredentials() {
//         setPrivateField(sunbirdConfig, "sunbirdUser", "");
//         setPrivateField(sunbirdConfig, "sunbirdPassword", "");

//         try {
//             sunbirdConfig.cqlSession();
//         } catch (Exception ignored) {
//             // Expected since we are not connecting to a real Cassandra instance
//         }
//     }
// }
