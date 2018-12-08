package io.getquill.dsl

import scala.language.implicitConversions
import scala.language.experimental.macros
import io.getquill.ast._
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.macros.whitebox.{ Context => MacroContext }
import io.getquill.util.Messages._
import scala.annotation.tailrec

class DynamicQueryDslMacro(val c: MacroContext) {
  import c.universe._
  
  def dynamicUnquote(d: Tree): Tree =
    q"${c.prefix}.unquote($d.q)"
}

trait DynamicQueryDsl {
  dsl: CoreDsl =>
    
  implicit def dynamicUnquote[T](d: DynamicQuery[T]): Query[T] = macro DynamicQueryDslMacro.dynamicUnquote

  implicit def toQuoted[T](q: DynamicQuery[T]): Quoted[Query[T]] = q.q
  implicit def toQuoted[T <: Action[_]](q: DynamicAction[T]): Quoted[T] = q.q

  def dynamicQuery[T](implicit t: TypeTag[T]): DynamicEntityQuery[T] =
    dynamicQuerySchema(t.tpe.typeSymbol.name.decodedName.toString)

  case class DynamicAlias[T](property: Quoted[T] => Quoted[Any], name: String)

  def alias[T](property: Quoted[T] => Quoted[Any], name: String): DynamicAlias[T] = DynamicAlias(property, name)

  case class DynamicSet[T, U](property: Quoted[T] => Quoted[U], value: U)(implicit val enc: Encoder[U])

  def set[T, U](property: Quoted[T] => Quoted[U], value: U)(implicit enc: Encoder[U]): DynamicSet[T, U] =
    DynamicSet(property, value)

  def set[T, U](property: String, value: U)(implicit enc: Encoder[U]): DynamicSet[T, U] =
    set(f => splice(Constant(property)), value)

  def dynamicQuerySchema[T](entity: String, columns: DynamicAlias[T]*): DynamicEntityQuery[T] = {
    val aliases =
      columns.map { alias =>

        @tailrec def path(ast: Ast, acc: List[String] = Nil): List[String] =
          ast match {
            case Property(a, name) =>
              path(a, name :: acc)
            case _ =>
              acc
          }

        PropertyAlias(path(alias.property(splice[T](Ident("v"))).ast), alias.name)
      }
    DynamicEntityQuery(splice[Query[T]](Entity(entity, aliases.toList)))
  }

  private def dyn[T](ast: Ast): DynamicQuery[T] =
    new DynamicQuery[T] {
      override def q = splice[Query[T]](ast)
    }

  private def splice[T](a: Ast) =
    new Quoted[T] {
      override def ast = a
    }

  protected def spliceLift[O](o: O)(implicit enc: Encoder[O]) =
    splice[O](ScalarValueLift("o", o, enc))

  sealed trait DynamicQuery[+T] {

    protected[getquill] def q: Quoted[Query[T]]
    
    private[this] def paramIdent[U, V](f: Quoted[U] => Quoted[V]) =

    protected[this] def transform[U, V, R](f: Quoted[U] => Quoted[V], t: (Ast, Ident, Ast) => Ast, r: Ast => R = dyn _) = {
      val v = Ident("v")
      r(t(q.ast, v, f(splice(v)).ast))
    }

    protected[this] def transformOpt[O, R, D <: DynamicQuery[T]](opt: Option[O], f: (Quoted[T], Quoted[O]) => Quoted[R], t: (Quoted[T] => Quoted[R]) => D, thiz: D)(implicit enc: Encoder[O]) =
      opt match {
        case Some(o) =>
          t(v => f(v, spliceLift(o)))
        case None =>
          thiz
      }

    def map[R](f: Quoted[T] => Quoted[R]): DynamicQuery[R] =
      transform(f, Map)

    def flatMap[R](f: Quoted[T] => Quoted[Query[R]]): DynamicQuery[R] =
      transform(f, FlatMap)

    def filter(f: Quoted[T] => Quoted[Boolean]): DynamicQuery[T] =
      transform(f, Filter)

    def withFilter(f: Quoted[T] => Quoted[Boolean]): DynamicQuery[T] =
      filter(f)

    def filterOpt[O](opt: Option[O])(f: (Quoted[T], Quoted[O]) => Quoted[Boolean])(implicit enc: Encoder[O]): DynamicQuery[T] =
      transformOpt(opt, f, filter, this)

    def concatMap[R, U](f: Quoted[T] => Quoted[U])(implicit ev: U => Traversable[R]): DynamicQuery[R] =
      transform(f, ConcatMap)

    def sortBy[R](f: Quoted[T] => Quoted[R])(implicit ord: Quoted[OrdDsl#Ord[R]]): DynamicQuery[T] =
      transform(f, SortBy(_, _, _, ord.ast))

    def take(n: Quoted[Int]): DynamicQuery[T] =
      dyn(Take(q.ast, n.ast))

    def take(n: Int)(implicit enc: Encoder[Int]): DynamicQuery[T] =
      take(spliceLift(n))

    def takeOpt(opt: Option[Int])(implicit enc: Encoder[Int]): DynamicQuery[T] =
      opt match {
        case Some(o) => take(o)
        case None    => this
      }

    def drop(n: Quoted[Int]): DynamicQuery[T] =
      dyn(Drop(q.ast, n.ast))

    def drop(n: Int)(implicit enc: Encoder[Int]): DynamicQuery[T] =
      drop(spliceLift(n))

    def dropOpt(opt: Option[Int])(implicit enc: Encoder[Int]): DynamicQuery[T] =
      opt match {
        case Some(o) => drop(o)
        case None    => this
      }

    def ++[U >: T](q2: Quoted[Query[U]]): DynamicQuery[U] =
      dyn(UnionAll(q.ast, q2.ast))

    def unionAll[U >: T](q2: Quoted[Query[U]]): DynamicQuery[U] =
      dyn(UnionAll(q.ast, q2.ast))

    def union[U >: T](q2: Quoted[Query[U]]): DynamicQuery[U] =
      dyn(Union(q.ast, q2.ast))

    def groupBy[R](f: Quoted[T] => Quoted[R]): DynamicQuery[(R, Query[T])] =
      transform(f, GroupBy)

    private def aggregate(op: AggregationOperator) =
      splice(Aggregation(op, q.ast))

    def min[U >: T]: Quoted[Option[T]] =
      aggregate(AggregationOperator.min)

    def max[U >: T]: Quoted[Option[T]] =
      aggregate(AggregationOperator.max)

    def avg[U >: T](implicit n: Numeric[U]): Quoted[Option[T]] =
      aggregate(AggregationOperator.avg)

    def sum[U >: T](implicit n: Numeric[U]): Quoted[Option[T]] =
      aggregate(AggregationOperator.sum)

    def size: Quoted[Long] =
      aggregate(AggregationOperator.size)

    def join[A >: T, B](q2: Quoted[Query[B]]): DynamicJoinQuery[A, B, (A, B)] =
      DynamicJoinQuery(InnerJoin, q, q2)

    def leftJoin[A >: T, B](q2: Quoted[Query[B]]): DynamicJoinQuery[A, B, (A, B)] =
      DynamicJoinQuery(LeftJoin, q, q2)

    def rightJoin[A >: T, B](q2: Quoted[Query[B]]): DynamicJoinQuery[A, B, (A, B)] =
      DynamicJoinQuery(RightJoin, q, q2)

    def fullJoin[A >: T, B](q2: Quoted[Query[B]]): DynamicJoinQuery[A, B, (A, B)] =
      DynamicJoinQuery(FullJoin, q, q2)

    private[this] def flatJoin[A >: T](tpe: JoinType, on: Quoted[A] => Quoted[Boolean]): DynamicQuery[A] = {
      val v = Ident("v")
      dyn(FlatJoin(tpe, q.ast, v, on(splice(v)).ast))
    }

    def join[A >: T](on: Quoted[A] => Quoted[Boolean]): DynamicQuery[A] =
      flatJoin(InnerJoin, on)

    def leftJoin[A >: T](on: Quoted[A] => Quoted[Boolean]): DynamicQuery[A] =
      flatJoin(LeftJoin, on)

    def rightJoin[A >: T](on: Quoted[A] => Quoted[Boolean]): DynamicQuery[A] =
      flatJoin(RightJoin, on)

    def nonEmpty: Quoted[Boolean] =
      splice(UnaryOperation(SetOperator.nonEmpty, q.ast))

    def isEmpty: Quoted[Boolean] =
      splice(UnaryOperation(SetOperator.isEmpty, q.ast))

    def contains[B >: T](value: B)(implicit enc: Encoder[B]): Quoted[Boolean] =
      contains(spliceLift(value))

    def contains[B >: T](value: Quoted[B]): Quoted[Boolean] =
      splice(BinaryOperation(q.ast, SetOperator.contains, value.ast))

    def distinct: DynamicQuery[T] =
      dyn(Distinct(q.ast))

    def nested: DynamicQuery[T] =
      dyn(Nested(q.ast))
    //
    //    def foreach[A <: Action[_], B](f: T => B)(implicit unquote: B => A): BatchAction[A]

    override def toString = q.toString
  }

  case class DynamicJoinQuery[A, B, R](tpe: JoinType, q1: Quoted[Query[A]], q2: Quoted[Query[B]]) {
    def on(f: (Quoted[A], Quoted[B]) => Quoted[Boolean]): DynamicQuery[R] = {
      val iA = Ident("a")
      val iB = Ident("b")
      dyn(Join(tpe, q1.ast, q2.ast, iA, iB, f(splice(iA), splice(iB)).ast))
    }
  }

  case class DynamicEntityQuery[T](q: Quoted[Query[T]])
    extends DynamicQuery[T] {

    private[this] def dyn[R](ast: Ast) =
      DynamicEntityQuery(splice[Query[R]](ast))

    override def filter(f: Quoted[T] => Quoted[Boolean]): DynamicEntityQuery[T] =
      transform(f, Filter, dyn)

    override def withFilter(f: Quoted[T] => Quoted[Boolean]): DynamicEntityQuery[T] =
      filter(f)

    override def filterOpt[O](opt: Option[O])(f: (Quoted[T], Quoted[O]) => Quoted[Boolean])(implicit enc: Encoder[O]): DynamicEntityQuery[T] =
      transformOpt(opt, f, filter, this)

    override def map[R](f: Quoted[T] => Quoted[R]): DynamicEntityQuery[R] =
      transform(f, Map, dyn)

    def insertValue(value: T)(implicit m: InsertMeta[T]): DynamicInsert[T] =
      new DynamicInsert[T] {
        override val q = splice[Insert[T]](FunctionApply(m.expand.ast, List(DynamicEntityQuery.this.q.ast, CaseClassValueLift("value", value))))
      }

    type DynamicAssignment[U] = ((Quoted[T] => Quoted[U]), U)

    private[this] def assignemnts[S](l: List[DynamicSet[S, _]]): List[Assignment] =
      l.map { s =>
        val v = Ident("v")
        Assignment(v, s.property(splice(v)).ast, ScalarValueLift("o", s.value, s.enc))
      }

    def insert(s: DynamicSet[T, _], l: DynamicSet[T, _]*): DynamicInsert[T] =
      new DynamicInsert[T] {
        override val q = splice(Insert(DynamicEntityQuery.this.q.ast, assignemnts(s :: l.toList)))
      }

    def updateValue(value: T)(implicit m: UpdateMeta[T]): DynamicUpdate[T] =
      DynamicUpdate(splice[Update[T]](FunctionApply(m.expand.ast, List(DynamicEntityQuery.this.q.ast, CaseClassValueLift("value", value)))))

    def update(s: DynamicSet[T, _], l: DynamicSet[T, _]*): DynamicUpdate[T] =
      DynamicUpdate(splice[Update[T]](Update(DynamicEntityQuery.this.q.ast, assignemnts(s :: l.toList))))

    def delete: DynamicDelete[T] =
      DynamicDelete(splice[Delete[T]](Delete(DynamicEntityQuery.this.q.ast)))
  }

  sealed trait DynamicAction[A <: Action[_]] {
    protected[getquill] def q: Quoted[A]

    override def toString = q.toString
  }

  trait DynamicInsert[E] extends DynamicAction[Insert[E]] {

    def returning[R](f: Quoted[E] => Quoted[R]): DynamicActionReturning[E, R] = {
      val v = Ident("v")
      DynamicActionReturning(splice(Returning(q.ast, v, f(splice(v)).ast)))
    }

    def onConflictIgnore: DynamicInsert[E] =
      new DynamicInsert[E] {
        val q = splice(OnConflict(DynamicInsert.this.q.ast, OnConflict.NoTarget, OnConflict.Ignore))
      }

    def onConflictIgnore(target: E => Any, targets: (E => Any)*): DynamicInsert[E] =
      new DynamicInsert[E] {
        val q = null
      }

    //    def onConflictUpdate(assign: ((E, E) => (Any, Any)), assigns: ((E, E) => (Any, Any))*): DynamicInsert[E]
    //
    //      @compileTimeOnly(NonQuotedException.message)
    //      def onConflictUpdate(target: E => Any, targets: (E => Any)*)(assign: ((E, E) => (Any, Any)), assigns: ((E, E) => (Any, Any))*): Insert[E] = NonQuotedException()
  }

  case class DynamicActionReturning[E, Output](q: Quoted[ActionReturning[E, Output]]) extends DynamicAction[ActionReturning[E, Output]]
  case class DynamicUpdate[E](q: Quoted[Update[E]]) extends DynamicAction[Update[E]]
  case class DynamicDelete[E](q: Quoted[Delete[E]]) extends DynamicAction[Delete[E]]
  //
  //  sealed trait BatchAction[+A <: Action[_]]
}
