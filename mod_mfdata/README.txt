This is an CSV import module which synchronizes data from CSV files to zotonic resources.

Currently the record structures are hardcoded.

Files:

mod_mfdata.erl 
-- 
The module's gen_server; uses the z_dropbox to watch for files in
the files/dropbox directory. When a file is spotted which has the
right filename, it looks up which record definition should be
imported, and processes the results. It e-mails the result of the
import to a predefined e-mail address (and also to the admin).

mfrecords.erl
--
High-level description of the CSV column names for 9 different
hardcoded CSV files: converts the CSV columns into erlang atom names.


mfdata.erl
--
The definition of the column mappings -- which dropbox file is linked
to which mfrecords record structure.

For every record structure there is a definition of which resources
should be created and what titles they should have:
import_definition/1.  The import_definition decides how the column
names map onto the zotonic field names and which page connections
(edges) should be created.


mfimport.erl
--
The import crunching script. This reads the mfdata and mfrecords
structures and does all the work, converting CSV files into zotonic
resources.

It can not only import the CSV files once, but by default it keeps the
zotonic pages that it manages in sync with the CSV files, e.g.,
deleting the zotonic pages when the records from the CSV files
disappear.


datamodel.pdf 
--
Block diagram of the datamodel that is imported with these scripts,
showing the different zotonic categories and how they relate.
