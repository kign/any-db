package net.inet_lab.any_db.loadrun;

import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;

import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.Logger;

class AnalyzeSql {
    private static final Logger log = Logger.getLogger(AnalyzeSql.class.getName());
    private final int defaultStringLen;
    private final String exportDriver;
    private final DBConnector dbConnector;
    private final Map<String,CType> ctCache;
    private List<String> tables;

    private AnalyzeSql(int defaultStringLen, DBConnector dbConnector, String exportDriver) {
        this.defaultStringLen = defaultStringLen;
        this.dbConnector = dbConnector;
        this.exportDriver = exportDriver;
        ctCache = new HashMap<>();
    }

    private CType getTableColType(String col) {
        if (ctCache.containsKey(col))
            return ctCache.get(col);
        CType ctype = null;
        for (String table : tables) {
            String t = dbConnector.getType(table, col);
            if (t == null)
                ctype = null;
            else if (t.equalsIgnoreCase("int"))
                ctype = new CType(CType.T_INT,4);
            else if (t.equalsIgnoreCase("bigint"))
                ctype = new CType(CType.T_INT,8);
            else if (t.equalsIgnoreCase("smallint"))
                ctype = new CType(CType.T_INT,2);
            else if (t.equalsIgnoreCase("tinyint"))
                ctype = new CType(CType.T_INT,1);
            else if (t.equalsIgnoreCase("boolean"))
                ctype = new CType(CType.T_BOOLEAN,1);
            else if (t.equalsIgnoreCase("string"))
                ctype = new CType(CType.T_STRING);
            else if (t.equalsIgnoreCase("double"))
                ctype = new CType(CType.T_FLOAT);
            else if (t.equalsIgnoreCase("timestamp"))
                ctype = new CType(CType.T_TIMESTAMP);
            else if (t.equalsIgnoreCase("date"))
                ctype = new CType(CType.T_DATE);
            else
                throw new RuntimeException("Type " + t + " not implemented");
        }
        ctCache.put(col, ctype);
        return ctype;
    }

    static String getSpec(int defaultStringLen, DBConnector dbConnector, String query, String exportDriver) {
        AnalyzeSql analyzeSql = new AnalyzeSql(defaultStringLen, dbConnector, exportDriver);
        return analyzeSql._getSpec(query);
    }


    private String _getSpec(String query) {
        Statements stmt;
        try {
            stmt = CCJSqlParserUtil.parseStatements(query);
        }
        catch (JSQLParserException e) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            e.printStackTrace(ps);
            String content = new String(baos.toByteArray(), StandardCharsets.UTF_8);
            String[] pc = content.split("\n");
            StringBuilder err = new StringBuilder();
            for (int ii = 0; ii < 15 && ii < pc.length; ii ++)
                err.append(pc[ii]).append("\n");

            log.error("Cannot parse query\n" + query + "\nError :" + err);
            return null;
        }

        List<Statement> statList = stmt.getStatements ();
        int n_stats = statList.size();
        if (n_stats > 1) {
            System.err.println("Accepting only one statements (detected " + n_stats + ")" );
            System.exit(1);
        }

        Statement stat = statList.get(0);

        if (! (stat instanceof Select)) {
            System.err.println("Statement is not SELECT" );
            System.exit(1);
        }

        Select sel = (Select) stat;
        SelectBody selBody = sel.getSelectBody();
        System.out.println(selBody);

        if (! (selBody instanceof PlainSelect)) {
            System.err.println("Statement is not plain SELECT" );
            System.exit(1);
        }

        PlainSelect psel = (PlainSelect) selBody;

        List<SelectItem>  items = psel.getSelectItems();

        System.out.println("" + items.size() + " selection items");

        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
	    tables = tablesNamesFinder.getTableList(sel);

        int nItems = items.size ();
        int idx = 0;
        StringBuilder spec = new StringBuilder();
        for (SelectItem item : items) {
            idx ++;
            if (! (item instanceof SelectExpressionItem)) {
                System.err.println("Item " + item + " is not SelectExpressionItem, auto spec generation isn't feasible");
                return null;
            }

            Alias a = ((SelectExpressionItem)item).getAlias();
            Expression e = ((SelectExpressionItem)item).getExpression();

            System.out.println("Alias: " + a + ", expression: " + e +
                               ", java type : " + e.getClass().getName() +
                               ", sql : " + columnName(e) + " " + columnType(e));

            spec.append(columnName(e)).append(" ").append(columnType(e));
            if (idx < nItems)
                spec.append(",");
        }

        return spec.toString();
    }

    private class CType {
        static final int T_STRING = 1;
        static final int T_INT = 2;
        static final int T_FLOAT = 3;
        static final int T_TIMESTAMP = 4;
        static final int T_DATE = 5;
        static final int T_BOOLEAN = 6;

        final int type;
        final int precision;

        CType(int type, int precision) {
            this.type = type;
            this.precision = precision;
        }

        CType(int type) {
            this.type = type;
            if (type == T_STRING)
                this.precision = defaultStringLen;
            else
                this.precision = 0;
        }

        public String toString() {
            if (type == T_STRING) {
                if ("snowflake".equalsIgnoreCase(exportDriver) ||
                        "presto".equalsIgnoreCase(exportDriver))
                    return "string";
                else
                    return "varchar(" + precision + ")";
            }
            else if (type == T_INT) {
                if (precision >= 8)
                    return "bigint";
                else if(precision == 4)
                    return "int";
                else if(precision <= 2)
                    return "smallint";
                else
                    return "int";
            }
            else if (type == T_FLOAT)
                return "float";
            else if (type == T_TIMESTAMP)
                return "timestamp without time zone";
            else if (type == T_DATE)
                return "date";
            else if (type == T_BOOLEAN)
                return "boolean";
            else
                return "unknown";
        }
    }

    private class ExpProc implements ExpressionVisitor {
        private String name = null;
        private CType ctype = null;
        public void visit(Column col) {
            name = col.getColumnName();
            ctype = getTableColType(name);
        }

        public void visit(Function fun) {
            String fname = fun.getName();
            List<Expression> args = fun.getParameters().getExpressions();

            if (fname.equalsIgnoreCase("substring")) {
                name = columnName(args.get(0));
                int prec = defaultStringLen;
                if (args.size() == 3 &&
                    args.get(1) instanceof LongValue &&
                    args.get(2) instanceof LongValue &&
                    ((LongValue)args.get(1)).getValue() == 1)
                    prec = (int)((LongValue)args.get(2)).getValue();
                ctype = new CType(CType.T_STRING,prec);
            }
            else {
                name = fname;
                log.error("No type info for function " + name);
            }
        }

        public void visit(Addition addition) {}
        public void visit(AllComparisonExpression allComparisonExpression) {}
        public void visit(AnalyticExpression aexpr) {}
        public void visit(AndExpression andExpression) {}
        public void visit(AnyComparisonExpression anyComparisonExpression) {}
        public void visit(Between between) {}
        public void visit(BitwiseAnd bitwiseAnd) {}
        public void visit(BitwiseOr bitwiseOr) {}
        public void visit(BitwiseXor bitwiseXor) {}
        public void visit(CaseExpression caseExpression) {}
        public void visit(CastExpression cast) {}
        public void visit(Concat concat) {}
        public void visit(DateTimeLiteralExpression literal) {}
        public void visit(NotExpression notExpression) {}
        public void visit(DateValue dateValue) {}
        public void visit(Division division) {}
        public void visit(DoubleValue doubleValue) {}
        public void visit(EqualsTo equalsTo) {}
        public void visit(ExistsExpression existsExpression) {}
        public void visit(ExtractExpression eexpr) {}
        public void visit(GreaterThan greaterThan) {}
        public void visit(GreaterThanEquals greaterThanEquals) {}
        public void visit(HexValue hexValue) {}
        public void visit(InExpression inExpression) {}
        public void visit(IntervalExpression iexpr) {}
        public void visit(IsNullExpression isNullExpression) {}
        public void visit(JdbcNamedParameter jdbcNamedParameter) {}
        public void visit(JdbcParameter jdbcParameter) {}
        public void visit(JsonExpression jsonExpr) {}
        public void visit(JsonOperator jsonOperator) {}
        public void visit(KeepExpression aexpr) {}
        public void visit(LikeExpression likeExpression) {}
        public void visit(LongValue longValue) {}
        public void visit(Matches matches) {}
        public void visit(MinorThan minorThan) {}
        public void visit(MinorThanEquals minorThanEquals) {}
        public void visit(Modulo modulo) {}
        public void visit(Multiplication multiplication) {}
        public void visit(MySQLGroupConcat groupConcat) {}
        public void visit(NotEqualsTo notEqualsTo) {}
        public void visit(NullValue nullValue) {}
        public void visit(NumericBind bind) {}
        public void visit(OracleHierarchicalExpression oexpr) {}
        public void visit(OracleHint hint) {}
        public void visit(OrExpression orExpression) {}
        public void visit(Parenthesis parenthesis) {}
        public void visit(RegExpMatchOperator rexpr) {}
        public void visit(RegExpMySQLOperator regExpMySQLOperator) {}
        public void visit(RowConstructor rowConstructor) {}
        public void visit(SignedExpression signedExpression) {}
        public void visit(StringValue stringValue) {}
        public void visit(SubSelect subSelect) {}
        public void visit(Subtraction subtraction) {}
        public void visit(TimeKeyExpression timeKeyExpression) {}
        public void visit(TimestampValue timestampValue) {}
        public void visit(TimeValue timeValue) {}
        public void visit(UserVariable var) {}
        public void visit(WhenClause whenClause) {}
        public void visit(WithinGroupExpression wgexpr) {}

        String getName() {
            if (name == null)
                return "unknown";
            return name;
        }

        CType getType() {
            if (ctype == null)
                return new CType(CType.T_STRING);
            return ctype;
        }
    }

    private String columnName (Expression e) {
        ExpProc ep = new ExpProc ();
        e.accept(ep);
        return ep.getName ();
    }

    private String columnType (Expression e) {
        ExpProc ep = new ExpProc ();
        e.accept(ep);
        return ep.getType().toString();
    }

}
