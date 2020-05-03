from google.cloud import bigquery

ADRES = [bigquery.SchemaField('TERC', 'STRING'),
         bigquery.SchemaField('SIMC', 'STRING'),
         bigquery.SchemaField('ULIC', 'STRING'),
         bigquery.SchemaField('Miejscowosc', 'STRING'),
         bigquery.SchemaField('Ulica', 'STRING'),
         bigquery.SchemaField('Budynek', 'STRING'),
         bigquery.SchemaField('Lokal', 'STRING'),
         bigquery.SchemaField('KodPocztowy', 'STRING'),
         bigquery.SchemaField('Poczta', 'STRING'),
         bigquery.SchemaField('Gmina', 'STRING'),
         bigquery.SchemaField('Powiat', 'STRING'),
         bigquery.SchemaField('Wojewodztwo', 'STRING'),
         bigquery.SchemaField('OpisNietypowegoMiejscaLokalizacji', 'STRING')]

CEIDG_SCHEMA = [bigquery.SchemaField('IdentyfikatorWpisu', 'STRING'),
                bigquery.SchemaField('DanePodstawowe', 'RECORD', 'NULLABLE', None,
                                     [bigquery.SchemaField('Imie', 'STRING'),
                                      bigquery.SchemaField('Nazwisko', 'STRING'),
                                      bigquery.SchemaField('NIP', 'STRING'),
                                      bigquery.SchemaField('REGON', 'STRING'),
                                      bigquery.SchemaField('Firma', 'STRING')]),
                bigquery.SchemaField('DaneKontaktowe', 'RECORD', 'NULLABLE', None,
                                     [bigquery.SchemaField('AdresPocztyElektronicznej', 'STRING'),
                                      bigquery.SchemaField('AdresStronyInternetowej', 'STRING'),
                                      bigquery.SchemaField('Telefon', 'STRING'),
                                      bigquery.SchemaField('Faks', 'STRING')]),
                bigquery.SchemaField('DaneAdresowe', 'RECORD', 'NULLABLE', None, [
                    bigquery.SchemaField('AdresGlownegoMiejscaWykonywaniaDzialalnosci', 'RECORD', 'NULLABLE', None,
                                         ADRES),
                    bigquery.SchemaField('AdresDoDoreczen', 'RECORD', 'NULLABLE', None,
                                         ADRES),
                    bigquery.SchemaField('PrzedsiebiorcaPosiadaObywatelstwaPanstw', 'STRING'),
                    bigquery.SchemaField('AdresyDodatkowychMiejscWykonywaniaDzialalnosci', 'STRING', 'REPEATED', None,
                                         ADRES)
                ]),
                bigquery.SchemaField('DaneDodatkowe', 'RECORD', 'NULLABLE', None,
                                     [bigquery.SchemaField('DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej',
                                                           'STRING'),
                                      bigquery.SchemaField('DataZawieszeniaWykonywaniaDzialalnosciGospodarczej',
                                                           'STRING'),
                                      bigquery.SchemaField('DataWznowieniaWykonywaniaDzialalnosciGospodarczej',
                                                           'STRING'),
                                      bigquery.SchemaField('DataZaprzestaniaWykonywaniaDzialalnosciGospodarczej',
                                                           'STRING'),
                                      bigquery.SchemaField('DataWykresleniaWpisuZRejeOptionalu', 'STRING', 'NULLABLE',
                                                           None, ()),
                                      bigquery.SchemaField('MalzenskaWspolnoscMajatkowa', 'STRING', 'NULLABLE', None,
                                                           ()),
                                      bigquery.SchemaField('Status', 'STRING'),
                                      bigquery.SchemaField('KodyPKD', 'STRING')]),
                bigquery.SchemaField('DataZgonu', 'STRING'),
                bigquery.SchemaField('Sukcesja', 'RECORD', fields=[
                    bigquery.SchemaField("Zarzadca", "RECORD", fields=[
                        bigquery.SchemaField("ImieZarzadcy", "STRING"),
                        bigquery.SchemaField("NazwiskoZarzadcy", "STRING"),
                        bigquery.SchemaField("NIP", "STRING"),
                        bigquery.SchemaField("ObywatelstwaZarzadcy", "STRING"),
                        bigquery.SchemaField("DataUstanowieniaZarzadcy", "STRING")
                    ]),
                    bigquery.SchemaField("DataUstanowieniaZarzadu", "STRING"),
                    bigquery.SchemaField("DataWygasnieciaZarzadu", "STRING")
                ]),

                bigquery.SchemaField('Zakazy', 'RECORD', fields=[
                    bigquery.SchemaField("InformacjaOZakazie", "RECORD", "REPEATED", fields=[
                        bigquery.SchemaField("Opis", "STRING"),
                        bigquery.SchemaField("DataUprawomocnieniaOrzeczenia", "STRING"),
                        bigquery.SchemaField("ZakazWydal", "STRING"),
                        bigquery.SchemaField("DataWydaniaOrzeczenia", "STRING"),
                        bigquery.SchemaField("Nazwa", "STRING"),
                        bigquery.SchemaField("OkresNaJakiZostalOrzeczonyZakaz", "STRING"),
                        bigquery.SchemaField("Typ", "STRING"),
                        bigquery.SchemaField("SygnaturaAktSprawy", "STRING")
                    ])
                ]),
                bigquery.SchemaField('SpolkiCywilneKtorychWspolnikiemJestPrzedsiebiorca', 'RECORD', fields=[
                    bigquery.SchemaField("InformacjeOSpolce", "RECORD", "REPEATED",fields=[
                        bigquery.SchemaField("NIP", "STRING"),
                        bigquery.SchemaField("REGON", "STRING")
                    ])
                ]),
                bigquery.SchemaField('InformacjeDotyczaceUpadlosciPostepowaniaNaprawczego', 'RECORD', fields=[
                    bigquery.SchemaField("Informacja", "RECORD", "REPEATED",fields=[
                        bigquery.SchemaField("DataOrzeczeniaWszczeciaPostepowaniaNaprawczego", "STRING"),
                        bigquery.SchemaField("RodzajInformacji", "STRING"),
                        bigquery.SchemaField("OrganWprowadzajacy", "STRING"),
                        bigquery.SchemaField("SygnaturaSprawy", "STRING"),
                    ])
                ]),
                bigquery.SchemaField('Uprawnienia', 'RECORD',fields=[
                    bigquery.SchemaField("Informacja", "STRING", "REPEATED")
                ])]
