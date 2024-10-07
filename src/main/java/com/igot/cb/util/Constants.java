package com.igot.cb.util;

/**
 * @author Mahesh RV
 */
public class Constants {

    public static final String KEYSPACE_SUNBIRD = "sunbird";
    public static final String KEYSPACE_SUNBIRD_COURSES = "sunbird_courses";
    public static final String CORE_CONNECTIONS_PER_HOST_FOR_LOCAL = "coreConnectionsPerHostForLocal";
    public static final String CORE_CONNECTIONS_PER_HOST_FOR_REMOTE = "coreConnectionsPerHostForRemote";
    public static final String MAX_CONNECTIONS_PER_HOST_FOR_LOCAL = "maxConnectionsPerHostForLocal";
    public static final String MAX_CONNECTIONS_PER_HOST_FOR_REMOTE = "maxConnectionsPerHostForRemote";
    public static final String MAX_REQUEST_PER_CONNECTION = "maxRequestsPerConnection";
    public static final String HEARTBEAT_INTERVAL = "heartbeatIntervalSeconds";
    public static final String POOL_TIMEOUT = "poolTimeoutMillis";
    public static final String CASSANDRA_CONFIG_HOST = "cassandra.config.host";
    public static final String SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL = "LOCAL_QUORUM";
    public static final String EXCEPTION_MSG_FETCH = "Exception occurred while fetching record from ";
    public static final String INSERT_INTO = "INSERT INTO ";
    public static final String DOT = ".";
    public static final String OPEN_BRACE = "(";
    public static final String VALUES_WITH_BRACE = ") VALUES (";
    public static final String QUE_MARK = "?";
    public static final String COMMA = ",";
    public static final String CLOSING_BRACE = ");";
    public static final String RESPONSE = "response";
    public static final String SUCCESS = "success";
    public static final String FAILED = "Failed";
    public static final String ERROR_MESSAGE = "errmsg";
    public static final String ERROR = "ERROR";
    public static final String REDIS_KEY_PREFIX = "cbextenroll_";
    public static final String CBPORES_REDIS_KEY_PREFIX = "cbpores_";
    public static final String DOT_SEPARATOR = ".";
    public static final String SHA_256_WITH_RSA = "SHA256withRSA";
    public static final String UNAUTHORIZED = "Unauthorized";
    public static final String SUB = "sub";
    public static final String SSO_URL = "sso.url";
    public static final String SSO_REALM = "sso.realm";
    public static final String ACCESS_TOKEN_PUBLICKEY_BASEPATH = "accesstoken.publickey.basepath";
    public static final String API_VERSION_1 = "1.0";
    public static final String X_AUTH_TOKEN = "x-authenticated-user-token";
    public static final String USER_ID_DOESNT_EXIST = "User Id doesn't exist! Please supply a valid auth token";
    public static final String TABLE_USER = "user";
    public static final String TABLE_USER_EXTERNAL_ENROLMENTS = "user_external_enrolments";
    public static final String ENROLLED_DATE = "enrolled_date";
    public static final String COURSE_ID_RQST = "courseId";
    public static final String CIOS_ENROLLMENT_CREATE="ciosenroll.v1.create";
    public static final String CIOS_ENROLLMENT_READ_COURSELIST="ciosenroll.v1.courselist.byuserid";
    public static final String CIOS_ENROLLMENT_READ_COURSEID="ciosenroll.v1.readby.useridcourseid";
    public static final String USER_ID="userid";
    public static final String COURSE_ID="courseid";
    public static final String STATUS="status";
    public static final String COMPLETED_ON="completedon";
    public static final String COMPLETION_PERCENTAGE="completionpercentage";
    public static final String UPDATED_ON="updatedon";
    public static final String PROVIDER_NAME="providerName";
    public static final String COURSE_NAME="courseName";
    public static final String COURSE_POSTER_IMAGE="coursePosterImage";
    public static final String RECIPIENT_NAME="recipientName";
    public static final String PROGRESS="progress";
    public static final String COMPLETION_DATE="completiondate";
    public static final String PARTNER_ID = "partnerId";
    public static final String CIOS_ENROLLMENT_PREGRESS_UPDATE="ciosprogressupdate.v1.create";



    private Constants() {
    }
}
