c = LOAD("https://raw.githubusercontent.com/acannistra/myria_similarity/master/data/cora/cora.txt",
            csv(schema(unknown:string,
            		   pubid:string,
                       author:string,
                       volume:string,
                       title:string,
                       institute:string,
                       venue:string,
                       address:string,
                       pub:string,
                       yr:string, -- "year" is a reserved keyword
                       pages:string,
                       editor:string,
                       note:string,
                       mnth:string, -- "month" is a reserved keyword
                       emptycol:string),
                skip=0, delimiter="\t"));

APPLY counter() {
-- function from MyriaL language docs
-- WARNING: every partition creates its 
-- own state (i.e. restarts the count) 
  [0 AS c];
  [c + 1];
  c;
};
Cora = [FROM c EMIT counter() as recordid, pubid, author, volume, title, institute, venue, address, pub, yr, pages, editor, note, mnth];

store(Cora, Cora);

Data = scan(Cora);

-- !!! Verify these `distinct`s
TitleNgrams = select distinct recordid, ngram(Data.title, 5) as ng FROM Data;
store(TitleNgrams, TitleNgrams);

AuthorNgrams = select distinct recordid, ngram(Data.author, 5) as ng from Data;
store(AuthorNgrams, AuthorNgrams);


-- this misses pairs that have no intersection –– do another kind of join
-- ERRORS HERE
intersection = [from AuthorNgrams a, AuthorNgrams b where a.ng=b.ng  emit a.recordid as aid, b.recordid as bid, count(*) as inter];

store(intersection, intersection);

-- union is reserved
--overlaps = [from AuthorNgrams a, AuthorNgrams b emit a.recordid, b.recordid, unionall([select aa.ng from AuthorNgrams aa where a.recordid = aa.recordid], [select bb.ng from AuthorNgrams bb where b.recordid = bb.recordid])]

counts = select recordid, count(ng) as cnt from AuthorNgrams;
store(counts, counts);

def jaccard(a, b, inter):
	inter / ((a+b)-inter);


jaccard = select a.recordid as a, b.recordid as b, jaccard(a.cnt, b.cnt, i.inter)
		  from counts a, counts b, intersection i
          where a.recordid = i.aid and b.recordid = i.bid;

store(jaccard, jaccard);


-- There are errors here.
