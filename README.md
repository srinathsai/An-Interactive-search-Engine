# An-Interactive-search-Engine

## Motivation :-
Let's imagine we are in the era before 1990 where there was no search Engine. So if you want any information, what would be the ideal place to look for the information? Yes, all the answers that popped in your minds are absolutely correct. We would definitely be engagging in a long tedius work of searching information from documents in a library. Therefore with this aspect,one should accept the fact that invention of search engine is no doubt a remarkable discovery of 20th century. Therefore it should be noted that designing a search engine is a lot of convoluted process which is very challenging and fun. <br />
**With a great passion towards problem solving and challenges , I have implemented the most famous information retrieval algorithms that are the back bone of search engine using pyspark which can be very effective in terms of time complexities on processing of big data.**


## Dataset used :-
a small sample of the pre-processed Wikipedia articles, with 10000s of  Wikipedia articles combined in to .txt file . <br />


## Methodolgy  :- 
+ First, from the data using **Pyspark rdds** we would store word1#doc-1,freq-1... format in a file. So in other words every line of a processed file indicates word frequencies in specific documents. But these processing has been done using the below models :- <br />

### Term Frequency model (TF) :
The TF algorithm counts **how many times a specific word appears in a document and assigns a score to it based on the number of occurrences.** <br />
Later this frequency is scaled down by applying log to the base 10 for uniformity.<br />
For example, let's say we have a document about cats and we want to know the importance of the word "meow." We count how many times "meow" appears in the document and let's say we find it appears 5 times. Then the term frequency score for "meow" in that document would be 1 + log(5).

### Cosine Normalized Term Frequency model (CTF) :
Cosine normalized term frequency is a variant of the term frequency (TF) metric that normalizes the raw term frequency by the total number of terms in a document. This normalization helps to reduce the impact of document length on the TF score and makes it more comparable across different documents.

The formula for cosine normalized term frequency for a term t in a document d is:
tf(t,d) = (number of times term t appears in document d) / (Square root of sum of frequencies of all).

+ Next, with the above models we will be having a file in every line is of the format word@doc-1#freq+doc-2#freq+doc-3#freq+... <br />
+ **With this file as input all the files which are named as Query takes a query or infinite queries from users preprocess it and returns a rdd in the format doc-1, value in decreasing order based on the value. This rdd is the desired output which signifies what are the documents with matching words in the query from the user.**
