package io.github.yeyuhl.database.cli.visitor;

import io.github.yeyuhl.database.Database;
import io.github.yeyuhl.database.Transaction;
import io.github.yeyuhl.database.cli.parser.ASTSQLStatementList;
import io.github.yeyuhl.database.cli.parser.ParseException;
import io.github.yeyuhl.database.cli.parser.RookieParser;
import io.github.yeyuhl.database.cli.visitor.StatementListVisitor;
import io.github.yeyuhl.database.cli.visitor.StatementType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import static org.junit.Assert.assertEquals;

public class TestStatementList {
    private static final String TestDir = "testSelectClause";
    private Database db;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.db = new Database(filename, 32);
        this.db.setWorkMem(16);
        try {
            db.loadDemo();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @After
    public void afterEach() {
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
        this.db.close();
    }

    public StatementListVisitor parse(String input) {
        RookieParser p = new RookieParser(new ByteArrayInputStream(input.getBytes()));
        ASTSQLStatementList node;
        try {
            node = p.sql_stmt_list();
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
        StatementListVisitor visitor = new StatementListVisitor(db, System.out);
        node.jjtAccept(visitor, null);
        return visitor;
    }

    @Test
    public void testSimpleQuery() {
        StatementListVisitor visitor = parse(
                "SELECT * FROM Students;"
        );
        assertEquals(1, visitor.statementVisitors.size());
        assertEquals(StatementType.SELECT, visitor.statementVisitors.get(0).getType());
    }

    @Test
    public void testSemicolonsA() {
        StatementListVisitor visitor = parse(
                ";;;SELECT * FROM Students;"
        );
        assertEquals(1, visitor.statementVisitors.size());
        assertEquals(StatementType.SELECT, visitor.statementVisitors.get(0).getType());
    }

    @Test
    public void testSemicolonsB() {
        StatementListVisitor visitor = parse(
                "SELECT * FROM Students;;;;;;;"
        );
        assertEquals(1, visitor.statementVisitors.size());
        assertEquals(StatementType.SELECT, visitor.statementVisitors.get(0).getType());
    }

    @Test
    public void testSemicolonsC() {
        StatementListVisitor visitor = parse(
                ";;; ;; ; SELECT * FROM Students;;\n;;\t;;;"
        );
        assertEquals(1, visitor.statementVisitors.size());
        assertEquals(StatementType.SELECT, visitor.statementVisitors.get(0).getType());
    }

    @Test
    public void testSemicolonsD() {
        StatementListVisitor visitor = parse(
                ";  ;; ;;\t; SELECT * FROM Students;;\n\n;;;EXPLAIN SELECT * FROM Students;;"
        );
        assertEquals(2, visitor.statementVisitors.size());
        assertEquals(StatementType.SELECT, visitor.statementVisitors.get(0).getType());
        assertEquals(StatementType.EXPLAIN, visitor.statementVisitors.get(1).getType());
    }
}
