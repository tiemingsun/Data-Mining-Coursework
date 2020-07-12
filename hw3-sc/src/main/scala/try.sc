val user_index:Map[Int, String] = Map(11 -> "a", 12 ->"b")
val index_user:Map[String, Int] = user_index.map{
  case (k,v) => (v,k)
}
