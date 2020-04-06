(TeX-add-style-hook
 "poster"
 (lambda ()
   (TeX-add-to-alist 'LaTeX-provided-class-options
                     '(("beamer" "final" "hyperref={pdfpagelabels=false}")))
   (TeX-add-to-alist 'LaTeX-provided-package-options
                     '(("sourcesanspro" "scaled=1") ("babel" "english") ("beamerposter" "orientation=portrait" "size=a0" "scale=1.4" "debug") ("acronym" "nolist") ("biblatex" "	style=authoryear-comp" "	backend=biber" "") ("hyphenat" "none")))
   (TeX-run-style-hooks
    "latex2e"
    "commands"
    "beamer"
    "beamer10"
    "sourcesanspro"
    "grffile"
    "babel"
    "amsmath"
    "amsthm"
    "amssymb"
    "latexsym"
    "wrapfig"
    "subfigure"
    "array"
    "booktabs"
    "tabularx"
    "beamerposter"
    "acronym"
    "siunitx"
    "tikz"
    "biblatex"
    "xspace"
    "setspace"
    "hyphenat")
   (TeX-add-symbols
    '("thispdfpagelabel" 1)
    "printabstract"
    "printconference")
   (LaTeX-add-labels
    "fig:unilogo"
    "tbl: errortable1")
   (LaTeX-add-bibliographies
    "literature")
   (LaTeX-add-lengths
    "columnheight"))
 :latex)

