package edu.whu.tmdb.query.excecute;

import net.sf.jsqlparser.statement.Statement;

import edu.whu.tmdb.query.utils.TMDBException;

public interface Create {
    boolean create(Statement stmt) throws TMDBException;

}
