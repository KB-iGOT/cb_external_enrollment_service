package com.igot.cb.util.dto;

public class SunbirdApiRespParam {

    private String resmsgid;
    private String msgid;
    private String err;
    private String status;
    private String msg;

    public String getResmsgid() {
        return resmsgid;
    }

    public void setResmsgid(String resmsgid) {
        this.resmsgid = resmsgid;
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public String getErr() {
        return err;
    }

    public void setErr(String err) {
        this.err = err;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public SunbirdApiRespParam() {
    }

    public SunbirdApiRespParam(String id) {
        resmsgid = id;
        msgid = id;
    }
}
