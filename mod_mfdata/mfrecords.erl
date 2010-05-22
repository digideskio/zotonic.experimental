%% @author Arjan Scherpenisse <arjan@scherpenisse.net>
%%
%% Semi-Record definitions for Mediafonds data imports.

-module(mfrecords).

-export([
         import_mf_grant/0,
         import_mf_maker/0,
         import_mf_broadcast/0,
         import_mf_award/0,
         import_mf_pp_project/0,
         import_mf_edition/0,
         import_mf_cluster/0,
         import_mf_mailing_weekoverzicht/0,
         import_mf_mailinglist/0
        ]).

         
%% Grant = "toekenning" (in their system: "aanvraag").
import_mf_grant() ->
    [
     %% Dossiernummer
     file_nr, 
     %% Werktitel
     title_working,
     %% Uitzendtitel
     title_broadcast,
     %% Laatste datum
     last_date,
     %% Categoriecode
     category_code,
     %% Subsidiecode
     grant_code,
     %% Aanvrager
     requestor,
     %% Extern producent
     external_producer,
     %% Beschrijving volgens aanvraag
     description,
     %% Beschrijving volgens uitzending
     description_broadcast,

     %% Stimuleringsbeleid code
     pp_project_code,

     %% Editie code
     edition_code,
     %% Cluster code
     cluster_code,
     %% URL
     url
    ].

%% Maker
import_mf_maker() ->
    [
     %% *unique id
     unique_id,
     %% Dossiernummer
     file_nr,
     %% Funktie (Filmplan; Radioplan; Scenario; Regie; Camera; Beoogd regisseur; Regie)
     function,
     %% Voornaam
     name_first,
     %% Voorvoegsel
     prefix,
     %% Achternaam
     name_last
    ].

%% Uitzending
import_mf_broadcast() ->
    [
     %% Dossiernummer
     file_nr,
     %% Zender
     channel,
     %% datum uitzending
     broadcast_date,
     %% begintijd
     time_start,
     %% eindtijd
     time_end,
     %% uitzendtitel
     title,
     %% ** beschruitz.
     description
    ].

%% Prijs
import_mf_award() ->
    [
     %% Dossiernummer
     file_nr,
     %% Organisatie die prijs toekent
     organisation,
     %% Naam van de prijs
     title,
     %% datum toekenning
     date
    ].

%% Project stimuleringsbeleid
import_mf_pp_project() ->
    [
     %% Code
     code,
     %% Titel
     title,
     %% Omschrijving
     description
    ].

%% Editie
import_mf_edition() ->
    [
     %% Code
     code,
     %% Project stimuleringsbeleid code
     pp_project_code,
     %% Titel
     title,
     %% Omschrijving
     description
    ].

%% Cluster
import_mf_cluster() ->
    [
     %% Code
     code,
     %% Titel
     title
    ].

%% Mailing
import_mf_mailinglist() ->
    [
     %% E-mail
     email
    ].

%% Mailing
import_mf_mailing_weekoverzicht() ->
    [
     %% E-mail
     email
    ].

