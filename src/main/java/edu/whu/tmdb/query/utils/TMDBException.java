package edu.whu.tmdb.query.utils;

public class TMDBException extends Exception{
//    private int errorCode;

    public TMDBException( String message) {
        super(message);
//        this.errorCode = errorCode;
    }

//    public int getErrorCode() {
//        return errorCode;
//    }
}
