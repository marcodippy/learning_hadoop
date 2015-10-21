# Exercise #

Some improvements on the [previous project](../average_grade)

Given a semicolon delimited file containing a list of grades of students in several courses (not ordered), calculate the average grade of each student for each course (each student has three grades per course). Output file must be comma separated and its record must be ordered per course (ascending order) and student (descending order).  
  
New requirements:
* Input file can contain some wrong input records (missing fields or invalid data: *grade* must be comprised between 60 and 100).
* We want to count also the number of:
 * distinct courses appearing in the file
 * distinct students 
 * students for each course
 * courses for each student