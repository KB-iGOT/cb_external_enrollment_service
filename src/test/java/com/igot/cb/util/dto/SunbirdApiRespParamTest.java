package com.igot.cb.util.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SunbirdApiRespParamTest {

    @Test
    void testNoArgConstructor() {
        SunbirdApiRespParam param = new SunbirdApiRespParam();
        assertNull(param.getResmsgid());
        assertNull(param.getMsgid());
    }

    @Test
    void testIdConstructor() {
        SunbirdApiRespParam param = new SunbirdApiRespParam("id1");
        assertEquals("id1", param.getResmsgid());
        assertEquals("id1", param.getMsgid());
    }

    @Test
    void testGettersAndSetters() {
        SunbirdApiRespParam param = new SunbirdApiRespParam();

        param.setResmsgid("resmsgid1");
        param.setMsgid("msgid1");
        param.setErr("err1");
        param.setStatus("status1");
        param.setMsg("msg1");

        assertEquals("resmsgid1", param.getResmsgid());
        assertEquals("msgid1", param.getMsgid());
        assertEquals("err1", param.getErr());
        assertEquals("status1", param.getStatus());
        assertEquals("msg1", param.getMsg());
    }
}
