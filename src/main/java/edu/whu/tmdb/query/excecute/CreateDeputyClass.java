package edu.whu.tmdb.query.excecute;

import net.sf.jsqlparser.statement.Statement;

import edu.whu.tmdb.query.utils.TMDBException;

import java.io.IOException;

public interface CreateDeputyClass {
    boolean createDeputyClass(Statement stmt) throws TMDBException, IOException;
}
