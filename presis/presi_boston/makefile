SHELL=/bin/bash

BASEFILE="main"
cc=xelatex

theme=beamerthemeWeizenbaum

.PHONY: all
all: gitsubmodules
	${cc} -synctex=1 $(BASEFILE)
	${cc} -synctex=1 $(BASEFILE)
	${cc} -synctex=1 $(BASEFILE)

.PHONY: allbib
allbib: gitsubmodules
	${cc} -synctex=1 $(BASEFILE)
	bibtex $(BASEFILE)
	${cc} -synctex=1 $(BASEFILE)
	${cc} -synctex=1 $(BASEFILE)

.PHONY: bib
bib:
	bibtex $(BASEFILE)

.PHONY: fast
fast: 
	${cc} -synctex=1 $(BASEFILE)

.PHONY: gitsubmodules
gitsubmodules:
	if [ ! -e ${theme}.sty ]; then ln -s weizenbaum-latex-theme-for-slides/${theme}.sty ${theme}.sty; fi
		git submodule update --init --recursive --remote

.PHONY: allclean
allclean: all clean

.PHONY: allcleaner
allcleaner: all cleaner

.PHONY: clean
clean:
	rm -f *.aux 
	rm -f *.bbl 
	rm -f *.blg 
	rm -f *.glo 
	rm -f *.idx 
	rm -f *.log 
	rm -f *.nav 
	rm -f *.out 
	rm -f *.snm 
	rm -f *.synctex.gz
	rm -f *.toc

.PHONY: cleaner
cleaner: clean
	rm -f ${FILE}.ps
	rm -f ${FILE}.pdf
	rm -f ${FILE}.dvi
	rm -f ${FILE}-blx.bib
	rm -f ${FILE}.run.xml

