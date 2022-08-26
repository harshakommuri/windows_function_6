create database win_fun
use win_fun

create table ineuron_students(
student_id int not null,
student_betch varchar(40),
student_name varchar(40),
student_stream varchar(40),
student_marks int,
student_mailid varchar(40))

describe ineuron_students

insert into ineuron_students values(101, 'fsda', 'saurabh', 'cs', 80, 'saurabh@gmail.com')
select * from ineuron_students
insert into ineuron_students values(100 ,'fsda' , 'saurabh','cs',80,'saurabh@gmail.com'),
(102 ,'fsda' , 'sanket','cs',81,'sanket@gmail.com'),
(103 ,'fsda' , 'shyam','cs',80,'shyam@gmail.com'),
(104 ,'fsda' , 'sanket','cs',82,'sanket@gmail.com'),
(105 ,'fsda' , 'shyam','ME',67,'shyam@gmail.com'),
(106 ,'fsds' , 'ajay','ME',45,'ajay@gmail.com'),
(106 ,'fsds' , 'ajay','ME',78,'ajay@gmail.com'),
(108 ,'fsds' , 'snehal','CI',89,'snehal@gmail.com'),
(109 ,'fsds' , 'manisha','CI',34,'manisha@gmail.com'),
(110 ,'fsds' , 'rakesh','CI',45,'rakesh@gmail.com'),
(111 ,'fsde' , 'anuj','CI',43,'anuj@gmail.com'),
(112 ,'fsde' , 'mohit','EE',67,'mohit@gmail.com'),
(113 ,'fsde' , 'vivek','EE',23,'vivek@gmail.com'),
(114 ,'fsde' , 'gaurav','EE',45,'gaurav@gmail.com'),
(115 ,'fsde' , 'prateek','EE',89,'prateek@gmail.com'),
(116 ,'fsde' , 'mithun','ECE',23,'mithun@gmail.com'),
(117 ,'fsbc' , 'chaitra','ECE',23,'chaitra@gmail.com'),
(118 ,'fsbc' , 'pranay','ECE',45,'pranay@gmail.com'),
(119 ,'fsbc' , 'sandeep','ECE',65,'sandeep@gmail.com')

-- There are two types of windowing functions, 1. aggregative, 2. Analytical
-- Aggregative: which aggregative the data(values) for a specific interval or for specific set of data.

select student_betch, sum(student_marks) from ineuron_students group by student_betch
select student_betch, min(student_marks) from ineuron_students group by student_betch
select student_betch, max(student_marks) from ineuron_students group by student_betch
select student_betch, avg(student_marks) from ineuron_students group by student_betch
select student_betch, count(*) from ineuron_students group by student_betch
-- in the above query, we try to get sum of marks by betch wise.
-- The above function can be called as aggregative windiwing function, as we are summing data for specific sets.

select student_name, student_marks from ineuron_students where student_marks in
(select max(student_marks) from ineuron_students where student_betch = 'fsda')
-- the above query will give us the student name who has highets marks from batch fsda.

select * from ineuron_students where student_betch = 'fsda' order by student_marks desc limit 1, 1
-- the above query is used to get the second highest marks in fsda batch
select * from ineuron_students where student_betch = 'fsda' order by student_marks desc limit 2, 2
-- the above query is used to get the 3rd and 4th highest marks in fsda batch

-- we can do the above the above activaties(functions ) with the help of analytical window function.
-- Analytical Windowing Function
select student_id , student_betch , student_stream, student_marks ,
row_number() over(order by student_marks desc) as 'row_number' from ineuron_students
-- here we used analytical windowing function and has given ranks as per marks
select student_id , student_betch , student_stream, student_marks ,
row_number() over(partition by student_betch order by student_marks desc ) as 'Rank' from ineuron_students
-- In the above query we had given ranks as above but divided them by batch's.
-- partition is the key word which creates different sets/bundles/windows.

select student_id , student_betch , student_stream, student_marks ,
row_number() over(order by student_marks desc) as 'row_number' from ineuron_students where student_betch = 'fsda'

-- find out highest rank person in every batch
select * from (select student_id , student_betch , student_stream, student_marks ,
row_number() over(partition by student_betch order by student_marks desc ) as 'Ranks' from ineuron_students) as test where Ranks = 1
-- this query will give answer, but it is not correct method. because if we have more then 2 rows with rank 1 it will show only 1. so, its not the right method

-- Using Rank
select * from (select student_id , student_betch , student_stream, student_marks ,
rank() over(partition by student_betch order by student_marks desc ) as 'row_ranks' from ineuron_students) as test where row_ranks= 3
-- Now this will give all the names who has rank as 3. But there is another problem. 	
select student_id , student_betch , student_stream, student_marks ,
row_number() over(partition by student_betch order by student_marks desc ) as 'row_number',
rank() over(partition by student_betch order by student_marks desc ) as 'row_ranks' from ineuron_students
-- The problem with using rank is if there are two rank 1's next it will give rank 3 for for second highest instead of 2. we can resolve this by using dense rank function.

-- Using Dense rank.
select student_id , student_betch , student_stream, student_marks ,
row_number() over(partition by student_betch order by student_marks desc ) as 'row_number',
rank() over(partition by student_betch order by student_marks desc ) as 'row_ranks',
dense_rank() over( partition by student_betch order by student_marks desc) as 'dense_rank' from ineuron_students
-- As you can see in output, our issue is completely resolved. 
-- Now we can get rank 2 even if there are morethen one rank 1's.

-- let's try getting the rank 4 details
select * from (select student_id , student_betch , student_stream, student_marks ,
row_number() over(partition by student_betch order by student_marks desc ) as 'row_number',
rank() over(partition by student_betch order by student_marks desc ) as 'row_ranks',
dense_rank() over( partition by student_betch order by student_marks desc) as 'row_dense_rank' from ineuron_students) as test where row_dense_rank=3
-- the above type of queries are called sub queries, a query inside a query.

select * from (select student_id , student_betch , student_stream, student_marks ,
row_number() over(partition by student_betch order by student_marks desc ) as 'row_number',
rank() over(partition by student_betch order by student_marks desc ) as 'row_ranks',
dense_rank() over( partition by student_betch order by student_marks desc) as 'row_dense_rank' from ineuron_students) as test where row_dense_rank in (1,2,3)



