# JumpShot_spark_script
Spark-shell script plus the input file :  

1) Save the input file in this folder - "/user/kshitijkumar068300/jumpshot_part-00000"
2) Spark-shell -i kk1.scala
3) Output is printed on the shell

####################################################################

input file :


147489900	3e7d3e64-84bc-11e6-b518-6c4008b1dad6	http://google.com/q=acme+deals
147490000	3e7d3e64-84bc-11e6-b518-6c4008b1dad6	http://cooldeals.com/acme-offer
147490014	03d79704-84bd-11e6-88ff-6c4008b1dad6	http://yahoo.com/
147490080	3e7d3e64-84bc-11e6-b518-6c4008b1dad6	http://acme.com/specials
147490095	3e7d3e64-84bc-11e6-b518-6c4008b1dad6	http://acme.com/checkout
147490095	2985b1e8-84bd-11e6-814c-6c4008b1dad6	http://facebook.com/abc
147490150	3e7d3e64-84bc-11e6-b518-6c4008b1dad6	http://acme.com/thank-you
147490150	3e7d3e64-84bc-11e6-b508-6c4008b1dad6	http://domain1.com/path1
147490152	3e7d3e64-84bc-11e6-b508-6c4008b1dad6	http://domain2.com/path2
147490153	3e7d3e64-84bc-11e6-b508-6c4008b1dad6	http://domain2.com/path3

Tuple having interesting clicks :

val intrstTup = sc.parallelize(List(("acme.com","/thank-you"),("cooldeals.com","/acme-offer"),("domain2.com","/path3")))


