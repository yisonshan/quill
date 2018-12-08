package io.getquill.quotation

import io.getquill._
import io.getquill.dsl.DynamicQueryDsl

class DynamicQuerySpec extends Spec {
  
  object testContext extends MirrorContext(MirrorIdiom, Literal) with TestEntities with DynamicQueryDsl
  import testContext._
  
  "test" in {
    
    val t = TestEntity("s", 1, 2L, Some(3))
    
    val a = "a"
    val aaa = "aaa"
    val q = dynamicQuerySchema[TestEntity](a, alias(_.i, aaa))
    testContext.run(q.insert(set(_.i, 1))).toString mustEqual null
//    
//    q.filterOpt(Some(0))((e, i) => quote(if(i < 0) false else e.i == i)).insert(t).ast.toString mustEqual null
//    
//    def test(shouldFilter: Boolean) =
//      q.filter(v => if(shouldFilter) v.i > 0 else true) 
      
  }
}