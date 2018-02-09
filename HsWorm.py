import urllib.request
import re
import urllib

class HsWorm:
    
    def readUrl(self,url):
        req=urllib.request.Request(url)
        html=urllib.request.urlopen(req)
        doc=html.read().decode('utf8')
        print(doc)
        return doc
    
    def downloadf(self,sUrl):
        urllib.urlretrieve(sUrl,"test.zip")
#         f=urllib2.urlopen(sUrl)
#         data=f.read()
#         with open("code.zip","wb") as code:
#             code.write(data)

        