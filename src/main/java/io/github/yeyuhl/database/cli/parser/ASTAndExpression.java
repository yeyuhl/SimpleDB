/* Generated By:JJTree: Do not edit this line. ASTAndExpression.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package io.github.yeyuhl.database.cli.parser;

public
class ASTAndExpression extends SimpleNode {
  public ASTAndExpression(int id) {
    super(id);
  }

  public ASTAndExpression(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=2cc6ff4c51319d79565273a0a0252a14 (do not edit this line) */
