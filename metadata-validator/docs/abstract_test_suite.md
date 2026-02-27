# Abstract test Suite

The elements below are tested on being available in the suggested cardinality and type.

| Element DC | Element ISO | Cardinality | Type | Codelist | Comment |
| --- | --- | --- | --- | --- | --- |
| identifier | fileidentifier | 1-n | string | - | |
| title | title | 1-n | string | - | |
| language | language | 0-n | string | - | 2/3/5-letter iso? |
| description | abstract | 0-n | string | | |
| date | date | 0-n | date | | |
| distribution | distributioninfo | 0-n | str or uri | | | 
| contributor | contact#? | 0-n | str or uri | | | 
| creator | contact#author | 0-n | str or uri | | | 
| publisher | contact#distributor | 0-n | str or uri | | | 
| coverage-temporal | extent#temporal | 0-n | date-period | | | 
| coverage-spatial | extent#spatial | 0-n | str, uri or bbox | | | 
| rights | constraints | 0-1 | str or uri | | | 
| license | constraints | 0-1 | str or uri | | | 
| subject | keyword/topiccategory | 0-n | str or uri | | | 
| type | hierarchylevel | 1-1 | str or uri | | | 
| format | format | 0-1 | str or uri | | | 

