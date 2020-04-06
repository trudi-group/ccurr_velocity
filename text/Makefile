LATEX=pdflatex
BIBTEX=bibtex
ifeq ($(USER),hwe)
	VIEWER=zeig.sh
else 				# generic
	VIEWER=evince
	PDFVIEWER=evince
endif

# define last changed file stem by default, to override: 
# make FILE=filename [w/o .tex]
FILE=$(shell ls -c *.tex | head -1 | sed 's/.tex//g' )


pdf: 	
	$(LATEX) $(FILE).tex

bib:	
	$(BIBTEX) $(FILE)

all:	pdf bib pdf pdf

view:	pdf
	$(VIEWER) $(FILE).pdf &

log:	# show complete log-file
	cat $(FILE).log

clean:
	rm -f *.aux
	rm -f *.bak
	rm -f *.bbl
	rm -f *.blg
	rm -f *.lof
	rm -f *.log
	rm -f *.lol
	rm -f *.lot
	rm -f *.nav
	rm -f *.out
	rm -f *.RData
	rm -f *.Rout
	rm -f *.snm
	rm -f *.spl
	rm -f *.tmp
	rm -f *.toc
	rm -f *.synctex.gz
	rm -f *~

cleaner: clean
	rm -f $(FILE).pdf
	rm -f $(FILE).ps
	rm -f $(FILE).dvi

better: 
	emacs $(FILE).tex
