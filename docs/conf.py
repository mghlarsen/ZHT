# -*- coding: utf-8 -*-
import sys, os
from zht.version import packageVersion

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.viewcode']
templates_path = ['templates']
source_suffix = '.rst'
master_doc = 'index'
project = u'ZHT'
copyright = u'2011, Michael Larsen'
version = packageVersion
release = packageVersion
pygments_style = 'sphinx'

html_theme = 'default'
html_static_path = ['static']
htmlhelp_basename = 'ZHTdoc'

latex_elements = {
}

latex_documents = [
  ('index', 'ZHT.tex', u'ZHT Documentation',
   u'Michael Larsen', 'manual'),
]

man_pages = [
    ('index', 'zht', u'ZHT Documentation',
     [u'Michael Larsen'], 1)
]

texinfo_documents = [
  ('index', 'ZHT', u'ZHT Documentation',
   u'Michael Larsen', 'ZHT', 'One line description of project.',
   'Miscellaneous'),
]

