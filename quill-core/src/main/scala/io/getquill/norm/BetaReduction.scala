package io.getquill.norm

import io.getquill.ast._

case class BetaReduction(map: collection.Map[Ast, Ast])
  extends StatelessTransformer {

  override def apply(ast: Ast) =
    ast match {

      case ast if (map.contains(ast)) =>
        BetaReduction(map - ast - map(ast))(map(ast))
        
      case Property(Tuple(values), name) =>
        val aliases = values.distinct
        aliases match {
          case alias :: Nil if values.size > 1 =>
            super.apply(Property(alias, name))
          case _ => apply(values(name.drop(1).toInt - 1))
        }

      case Property(CaseClass(tuples), name) =>
        apply(tuples.toMap.apply(name))

      case FunctionApply(Function(params, body), values) =>
        val conflicts = values.flatMap(CollectAst.byType[Ident]).map { i =>
          i -> Ident(s"tmp_${i.name}")
        }.toMap[Ast, Ast]
        val newParams = params.map { p =>
          conflicts.getOrElse(p, p)
        }
        val bodyr = BetaReduction(conflicts ++ params.zip(newParams)).apply(body)
        apply(BetaReduction(map ++ newParams.zip(values).toMap).apply(bodyr))

      case Function(params, body) =>
        val newParams = params.map { p =>
          (map.get(p) match {
            case Some(i: Ident) => i
            case _              => p
          })
        }
        Function(newParams, BetaReduction(map ++ params.zip(newParams))(body))

      case Block(statements) =>
        apply {
          statements.reverse.tail.foldLeft((collection.Map[Ast, Ast](), statements.last)) {
            case ((map, stmt), line) =>
              BetaReduction(map)(line) match {
                case Val(name, body) =>
                  val newMap = map + (name -> body)
                  val newStmt = BetaReduction(stmt, newMap.toSeq: _*)
                  (newMap, newStmt)
                case other =>
                  (map, stmt)
              }
          }._2
        }

      case Foreach(query, alias, body) =>
        Foreach(query, alias, BetaReduction(map - alias)(body))

      case Returning(action, alias, prop) =>
        val t = BetaReduction(map - alias)
        Returning(apply(action), alias, t(prop))

      case other =>
        super.apply(other)
    }

  override def apply(o: OptionOperation) =
    o match {
      case other @ OptionFlatMap(a, b, c) =>
        OptionFlatMap(apply(a), b, BetaReduction(map - b)(c))
      case OptionMap(a, b, c) =>
        OptionMap(apply(a), b, BetaReduction(map - b)(c))
      case OptionForall(a, b, c) =>
        OptionForall(apply(a), b, BetaReduction(map - b)(c))
      case OptionExists(a, b, c) =>
        OptionExists(apply(a), b, BetaReduction(map - b)(c))
      case other =>
        super.apply(other)
    }

  override def apply(e: Assignment) =
    e match {
      case Assignment(alias, prop, value) =>
        val t = BetaReduction(map - alias)
        Assignment(alias, t(prop), t(value))
    }

  override def apply(query: Query) =
    query match {
      case Filter(a, b, c) =>
        Filter(apply(a), b, BetaReduction(map - b)(c))
      case Map(a, b, c) =>
        Map(apply(a), b, BetaReduction(map - b)(c))
      case FlatMap(a, b, c) =>
        FlatMap(apply(a), b, BetaReduction(map - b)(c))
      case ConcatMap(a, b, c) =>
        ConcatMap(apply(a), b, BetaReduction(map - b)(c))
      case SortBy(a, b, c, d) =>
        SortBy(apply(a), b, BetaReduction(map - b)(c), d)
      case GroupBy(a, b, c) =>
        GroupBy(apply(a), b, BetaReduction(map - b)(c))
      case Join(t, a, b, iA, iB, on) =>
        Join(t, apply(a), apply(b), iA, iB, BetaReduction(map - iA - iB)(on))
      case FlatJoin(t, a, iA, on) =>
        FlatJoin(t, apply(a), iA, BetaReduction(map - iA)(on))
      case _: Take | _: Entity | _: Drop | _: Union | _: UnionAll | _: Aggregation | _: Distinct | _: Nested =>
        super.apply(query)
    }
}

object BetaReduction {

  def apply(ast: Ast, t: (Ast, Ast)*): Ast =
    BetaReduction(t.toMap)(ast) match {
      case `ast` => ast
      case other => apply(other)
    }
}
