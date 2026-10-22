package com.igot.cb.util.exceptions;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class CustomException extends RuntimeException{

  private final String code;
  private final String message;
  private final HttpStatus httpStatusCode;

  public CustomException() {
    this(null, null, null);
  }

  public CustomException(String code, String message, HttpStatus httpStatusCode) {
    this.code = code;
    this.message = message;
    this.httpStatusCode = httpStatusCode;
  }

}
