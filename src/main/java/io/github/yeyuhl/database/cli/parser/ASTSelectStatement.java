/* Generated By:JJTree: Do not edit this line. ASTSelectStatement.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package io.github.yeyuhl.database.cli.parser;

public
class ASTSelectStatement extends SimpleNode {
  public ASTSelectStatement(int id) {
    super(id);
  }

  public ASTSelectStatement(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=47cf435725fb32d51b426dd9a4c23d13 (do not edit this line) */
