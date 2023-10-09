import pyodbc
from config import MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USERNAME, MSSQL_PASSWORD
from datetime import datetime

def get_mssql_connection():
    return pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD}')


def state_to_city(state):
    # Türkiye'nin il kodlarına karşılık gelen illeri bir sözlükte tutuyoruz.
    city_map = {
        "TR01": "ADANA", "TR02": "ADIYAMAN", "TR03": "AFYON", "TR04": "AĞRI", "TR05": "AMASYA", "TR06": "ANKARA",
        "TR07": "ANTALYA", "TR08": "ARTVİN", "TR09": "AYDIN", "TR10": "BALIKESİR", "TR11": "BİLECİK", "TR12": "BİNGÖL",
        "TR13": "BİTLİS", "TR14": "BOLU", "TR15": "BURDUR", "TR16": "BURSA", "TR17": "ÇANAKKALE", "TR18": "ÇANKIRI",
        "TR19": "ÇORUM", "TR20": "DENİZLİ", "TR21": "DİYARBAKIR", "TR22": "EDİRNE", "TR23": "ELAZIĞ", "TR24": "ERZİNCAN",
        "TR25": "ERZURUM", "TR26": "ESKİŞEHİR", "TR27": "GAZİANTEP", "TR28": "GİRESUN", "TR29": "GÜMÜŞHANE", "TR30": "HAKKARİ",
        "TR31": "HATAY", "TR32": "ISPARTA", "TR33": "MERSİN", "TR34": "İSTANBUL", "TR35": "İZMİR", "TR36": "KARS",
        "TR37": "KASTAMONU", "TR38": "KAYSERİ", "TR39": "KIRKLARELİ", "TR40": "KIRŞEHİR", "TR41": "KOCAELİ", "TR42": "KONYA",
        "TR43": "KÜTAHYA", "TR44": "MALATYA", "TR45": "MANİSA", "TR46": "KAHRAMANMARAŞ", "TR47": "MARDİN", "TR48": "MUĞLA",
        "TR49": "MUŞ", "TR50": "NEVŞEHİR", "TR51": "NİĞDE", "TR52": "ORDU", "TR53": "RİZE", "TR54": "SAKARYA",
        "TR55": "SAMSUN", "TR56": "SİİRT", "TR57": "SİNOP", "TR58": "SİVAS", "TR59": "TEKİRDAĞ", "TR60": "TOKAT",
        "TR61": "TRABZON", "TR62": "TUNCELİ", "TR63": "ŞANLIURFA", "TR64": "UŞAK", "TR65": "VAN", "TR66": "YOZGAT",
        "TR67": "ZONGULDAK", "TR68": "AKSARAY", "TR69": "BAYBURT", "TR70": "KARAMAN", "TR71": "KIRIKKALE", "TR72": "BATMAN",
        "TR73": "ŞIRNAK", "TR74": "BARTIN", "TR75": "ARDAHAN", "TR76": "IĞDIR", "TR77": "YALOVA", "TR78": "KARABÜK",
        "TR79": "KİLİS", "TR80": "OSMANİYE", "TR81": "DÜZCE"
    }
    return city_map.get(state, "")  # Eğer sözlükte il kodu bulunmazsa boş string döndürülür.



def get_or_create_customer(data, email):
    with get_mssql_connection() as conn:
        cursor = conn.cursor()
        print(f"WooCommerce'dan alınan e-posta adresi: {email}")

        # 1. Adım: E-posta adresini ara
        cursor.execute("SELECT cari_kod FROM CARI_HESAPLAR WHERE cari_EMail = ?", email)
        result = cursor.fetchone()

        if result:
            # 2. Adım: Eğer bulunursa, cari_kod'u döndür
            print(f"Database'de {email} e-posta adresi bulundu. Cari kod: {result[0]}")
            return result[0]
        else:
            # 3. Adım: Eğer bulunamazsa, yeni cari_kod oluştur
            print(f"Database'de {email} e-posta adresi bulunamadı. Yeni cari kaydı oluşturuluyor...")
            cursor.execute("SELECT MAX(cari_kod) FROM CARI_HESAPLAR WHERE cari_kod LIKE '120.04.%'")
            last_cari_kod = cursor.fetchone()[0]
            if last_cari_kod:
                last_num = int(last_cari_kod.split(".")[2])
                new_num = str(last_num + 1).zfill(4)
            else:
                new_num = "0001"
            new_cari_kod = f"120.04.{new_num}"
            print(f"Yeni cari kod : {new_cari_kod}")


        
        # Eğer company değeri boşsa, first_name ve last_name değerlerini kullanarak cari_unvan oluşturalım
        company = data.get("billing", {}).get("company")
        first_name = data.get("billing", {}).get("first_name")
        last_name = data.get("billing", {}).get("last_name")
        cari_unvan = company if company else f"{first_name} {last_name}"
        phone = data.get("billing", {}).get("phone")
        phone_raw = phone.strip()  # Telefon numarasındaki baştaki ve sondaki boşlukları kaldır
        phone_clean = phone_raw[1:] if phone_raw.startswith("0") else phone_raw  # Başındaki 0'ı kaldır
        state = data.get("shipping", {}).get("state")
        city = state_to_city(state)
        postcode = data.get("shipping", {}).get("postcode")
        shipping_address = data.get("shipping", {}).get("address_1")
        
        # meta_data'dan gerekli bilgileri çekelim
        tc_kimlik_no = ""
        vergi_dairesi = ""
        for item in data.get("meta_data", []):
            if item["key"] == "_billing_tc_kimlik_no_vergi_no":
                tc_kimlik_no = item["value"]
            elif item["key"] == "_billing_vergi_dairesi_zorunlu_deil_":
                vergi_dairesi = item["value"]
                print("Vergi Dairesi (Döngü İçinde):", vergi_dairesi)

            
            # Yeni müşteri kaydını oluştur
            sql = f"""INSERT INTO MikroDB_V16_2022_01.dbo.CARI_HESAPLAR ( cari_Guid, cari_DBCno, cari_SpecRECno, cari_iptal, cari_fileid, cari_hidden, cari_kilitli, cari_degisti, cari_checksum, cari_create_user, cari_create_date, cari_lastup_user, cari_lastup_date, cari_special1, cari_special2, cari_special3, cari_kod, cari_unvan1, cari_unvan2, cari_hareket_tipi, cari_baglanti_tipi, cari_stok_alim_cinsi, cari_stok_satim_cinsi, cari_muh_kod, cari_muh_kod1, cari_muh_kod2, cari_doviz_cinsi, cari_doviz_cinsi1, cari_doviz_cinsi2, cari_vade_fark_yuz, cari_vade_fark_yuz1, cari_vade_fark_yuz2, cari_KurHesapSekli, cari_vdaire_adi, cari_vdaire_no, cari_sicil_no, cari_VergiKimlikNo, cari_satis_fk, cari_odeme_cinsi, cari_odeme_gunu, cari_odemeplan_no, cari_opsiyon_gun, cari_cariodemetercihi, cari_fatura_adres_no, cari_sevk_adres_no, cari_banka_tcmb_kod1, cari_banka_tcmb_subekod1, cari_banka_tcmb_ilkod1, cari_banka_hesapno1, cari_banka_swiftkodu1, cari_banka_tcmb_kod2, cari_banka_tcmb_subekod2, cari_banka_tcmb_ilkod2, cari_banka_hesapno2, cari_banka_swiftkodu2, cari_banka_tcmb_kod3, cari_banka_tcmb_subekod3, cari_banka_tcmb_ilkod3, cari_banka_hesapno3, cari_banka_swiftkodu3, cari_banka_tcmb_kod4, cari_banka_tcmb_subekod4, cari_banka_tcmb_ilkod4, cari_banka_hesapno4, cari_banka_swiftkodu4, cari_banka_tcmb_kod5, cari_banka_tcmb_subekod5, cari_banka_tcmb_ilkod5, cari_banka_hesapno5, cari_banka_swiftkodu5, cari_banka_tcmb_kod6, cari_banka_tcmb_subekod6, cari_banka_tcmb_ilkod6, cari_banka_hesapno6, cari_banka_swiftkodu6, cari_banka_tcmb_kod7, cari_banka_tcmb_subekod7, cari_banka_tcmb_ilkod7, cari_banka_hesapno7, cari_banka_swiftkodu7, cari_banka_tcmb_kod8, cari_banka_tcmb_subekod8, cari_banka_tcmb_ilkod8, cari_banka_hesapno8, cari_banka_swiftkodu8, cari_banka_tcmb_kod9, cari_banka_tcmb_subekod9, cari_banka_tcmb_ilkod9, cari_banka_hesapno9, cari_banka_swiftkodu9, cari_banka_tcmb_kod10, cari_banka_tcmb_subekod10, cari_banka_tcmb_ilkod10, cari_banka_hesapno10, cari_banka_swiftkodu10, cari_EftHesapNum, cari_Ana_cari_kodu, cari_satis_isk_kod, cari_sektor_kodu, cari_bolge_kodu, cari_grup_kodu, cari_temsilci_kodu, cari_muhartikeli, cari_firma_acik_kapal, cari_BUV_tabi_fl, cari_cari_kilitli_flg, cari_etiket_bas_fl, cari_Detay_incele_flg, cari_efatura_fl, cari_POS_ongpesyuzde, cari_POS_ongtaksayi, cari_POS_ongIskOran, cari_kaydagiristarihi, cari_KabEdFCekTutar, cari_hal_caritip, cari_HalKomYuzdesi, cari_TeslimSuresi, cari_wwwadresi, cari_EMail, cari_CepTel, cari_VarsayilanGirisDepo, cari_VarsayilanCikisDepo, cari_Portal_Enabled, cari_Portal_PW, cari_BagliOrtaklisa_Firma, cari_kampanyakodu, cari_b_bakiye_degerlendirilmesin_fl, cari_a_bakiye_degerlendirilmesin_fl, cari_b_irsbakiye_degerlendirilmesin_fl, cari_a_irsbakiye_degerlendirilmesin_fl, cari_b_sipbakiye_degerlendirilmesin_fl, cari_a_sipbakiye_degerlendirilmesin_fl, cari_KrediRiskTakibiVar_flg, cari_ufrs_fark_muh_kod, cari_ufrs_fark_muh_kod1, cari_ufrs_fark_muh_kod2, cari_odeme_sekli, cari_TeminatMekAlacakMuhKodu, cari_TeminatMekAlacakMuhKodu1, cari_TeminatMekAlacakMuhKodu2, cari_TeminatMekBorcMuhKodu, cari_TeminatMekBorcMuhKodu1, cari_TeminatMekBorcMuhKodu2, cari_VerilenDepozitoTeminatMuhKodu, cari_VerilenDepozitoTeminatMuhKodu1, cari_VerilenDepozitoTeminatMuhKodu2, cari_AlinanDepozitoTeminatMuhKodu, cari_AlinanDepozitoTeminatMuhKodu1, cari_AlinanDepozitoTeminatMuhKodu2, cari_def_efatura_cinsi, cari_otv_tevkifatina_tabii_fl, cari_KEP_adresi, cari_efatura_baslangic_tarihi, cari_mutabakat_mail_adresi, cari_mersis_no, cari_istasyon_cari_kodu, cari_gonderionayi_sms, cari_gonderionayi_email, cari_eirsaliye_fl, cari_eirsaliye_baslangic_tarihi, cari_vergidairekodu, cari_CRM_sistemine_aktar_fl, cari_efatura_xslt_dosya, cari_pasaport_no, cari_kisi_kimlik_bilgisi_aciklama_turu, cari_kisi_kimlik_bilgisi_diger_aciklama, cari_uts_kurum_no, cari_kamu_kurumu_fl, cari_earsiv_xslt_dosya, cari_Perakende_fl, cari_yeni_dogan_mi, cari_eirsaliye_xslt_dosya, cari_def_eirsaliye_cinsi, cari_ozel_butceli_kurum_carisi, cari_nakakincelenmesi, cari_vergimukellefidegil_mi, cari_tasiyicifirma_cari_kodu, cari_nacekodu_1, cari_nacekodu_2, cari_nacekodu_3, cari_sirket_turu, cari_baba_adi, cari_faal_terk ) VALUES ( NEWID(), 0, 0, 0, 31, 0, 0, 0, 0, 1, GETDATE(), 1, GETDATE(),N'',N'',N'', '{new_cari_kod}', '{cari_unvan}', '{cari_unvan}', 0, 0, 0, 0,N'',N'',N'', 0, 255, 255, 25.000000000000, 0, 0, 1,N'',N'',N'',N'', 1, 0, 0, 0, 0, 0, 1, 1,N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'',N'', 1,N'',N'',N'',N'',N'',N'',N'', 0, 0, 0, 0, 0, 0, 0, 0, 0, '20231009', 0, 0, 0, 0,N'',N'',N'', 0, 0, 0,N'', 0,N'', 0, 0, 0, 0, 0, 0, 0,N'',N'',N'', 0,'910',N'',N'','912',N'',N'','226',N'',N'','326',N'',N'', 1, 0,N'', '18991231',N'',N'',N'', 0, 0, 0, '18991231',N'', 0,N'',N'', 0,N'',N'', 0,N'', 0, 0,N'', 0,N'', 0, 0,N'',N'',N'',N'', 0,N'', 0 )"""

            cursor.execute(sql)

            sql_insert = f"""
            INSERT INTO MikroDB_V16_2022_01.dbo.CARI_HESAP_ADRESLERI 
            (adr_Guid, adr_DBCno, adr_SpecRECno, adr_iptal, adr_fileid, adr_hidden, adr_kilitli, adr_degisti, adr_checksum, adr_create_user, adr_create_date, adr_lastup_user, adr_lastup_date, adr_special1, adr_special2, adr_special3, adr_cari_kod, adr_adres_no, adr_aprint_fl, adr_cadde, adr_mahalle, adr_sokak, adr_Semt, adr_Apt_No, adr_Daire_No, adr_posta_kodu, adr_ilce, adr_il, adr_ulke, adr_Adres_kodu, adr_tel_ulke_kodu, adr_tel_bolge_kodu, adr_tel_no1, adr_tel_no2, adr_tel_faxno, adr_tel_modem, adr_yon_kodu, adr_uzaklik_kodu, adr_temsilci_kodu, adr_ozel_not, adr_ziyaretperyodu, adr_ziyaretgunu, adr_gps_enlem, adr_gps_boylam, adr_ziyarethaftasi, adr_ziygunu2_1, adr_ziygunu2_2, adr_ziygunu2_3, adr_ziygunu2_4, adr_ziygunu2_5, adr_ziygunu2_6, adr_ziygunu2_7, adr_efatura_alias, adr_eirsaliye_alias)
            VALUES 
            (NEWID(), 0, 0, 0, 32, 0, 0, 0, 0, 1, GETDATE(), 1, GETDATE(),N'',N'',N'', '{new_cari_kod}', 1, 0, '{shipping_address}',N'',N'',N'',N'',N'', '{postcode}', '{state}', '{city}', 'TÜRKİYE',N'',N'',N'',N'',N'',N'',N'',N'', 0,N'',N'', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,N'',N'')
            """
            cursor.execute(sql_insert)

            

            sql_insert_muhasebe = f"""
            INSERT INTO MikroDB_V16_2022_01.dbo.MUHASEBE_HESAP_PLANI 
            (muh_Guid, muh_DBCno, muh_SpecRECno, muh_iptal, muh_fileid, muh_hidden, muh_kilitli, muh_degisti, muh_checksum, muh_create_user, muh_create_date, muh_lastup_user, muh_lastup_date, muh_special1, muh_special2, muh_special3, muh_hesap_kod, muh_hesap_isim1, muh_hesap_isim2, muh_hesap_tip, muh_doviz_cinsi, muh_kurfarki_fl, muh_sorum_merk, muh_kilittarihi, muh_hes_dav_bicimi, muh_kdv_tipi, muh_calisma_sekli, muh_maliyet_dagitim_sekli, muh_grupkodu, muh_enf_fark_maliyet_fl, muh_kdv_dagitim_sekli, muh_miktar_oto_fl, muh_ticariden_bilgi_girisi_fl, muh_proje_detayi, muh_kesin_mizan_hesap_kodu) 
            VALUES 
            (NEWID(), 0, 0, 0, 1, 0, 0, 0, 0, 1, GETDATE(), 1, GETDATE(),N'',N'',N'', '{new_cari_kod}', '{cari_unvan}',N'', 0, 0, 0, 0, '18991231', 0, 0, 0, 0,N'', 0, 0, 0, 0, 0,N'')
            """
            cursor.execute(sql_insert_muhasebe)

            sql_update = f"""
            UPDATE CARI_HESAPLAR
            SET cari_mutabakat_mail_adresi = '{email}', cari_gonderionayi_email = 1, cari_vdaire_no = '{tc_kimlik_no}', cari_EMail = '{email}', cari_CepTel = '{phone_clean}', cari_muh_kod = '{new_cari_kod}' WHERE cari_kod = '{new_cari_kod}'
            """
            cursor.execute(sql_update)


            conn.commit()
            return new_cari_kod


def save_order_to_mssql(order, email):
    with get_mssql_connection() as conn:
        cursor = conn.cursor()

        # WooCommerce'dan alınan id değerini alıp, başına "PCW-" ekleyin
        sip_belgeno = f"PCW-{order['id']}"

        # Kontrol: sipariş zaten veritabanında var mı?
        cursor.execute("SELECT COUNT(*) FROM SIPARISLER WHERE sip_belgeno = ?", (sip_belgeno,))
        if cursor.fetchone()[0] > 0:
            print(f"Sipariş belge no: {sip_belgeno} zaten veritabanında kaydedilmiş.")
            return

        # Burada önceki kodunuzu kısaltarak yerleştirdim
        tarih = "GETDATE()"
        sip_evrakno_seri = "PCW"
        current_date = datetime.now()
        formatted_date = current_date.strftime('%Y%m%d')
        formatted_date_without_time = current_date.strftime('%Y-%m-%d 00:00:00.000')
        
        # Sipariş serisine göre en son sıra numarasını bulalım
        cursor.execute("SELECT MAX(sip_evrakno_sira) FROM SIPARISLER WHERE sip_evrakno_seri=?", (sip_evrakno_seri,))
        last_sira = cursor.fetchone()[0]
        if last_sira is None:
            last_sira = 0
        sip_evrakno_sira = last_sira + 1
        
        sip_satirno = 0
        sip_musteri_kod = get_or_create_customer(order, email)
        
        for item in order['line_items']:
            sip_stok_kod = item['sku']
            
            # STOKLAR tablosundan sto_perakende_vergi bilgisini al
            cursor.execute("SELECT sto_perakende_vergi FROM STOKLAR WHERE sto_kod=?", (sip_stok_kod,))
            vergi_kodu = cursor.fetchone()[0]
            if vergi_kodu == 7:
                vergi_orani = 10
            elif vergi_kodu == 8:
                vergi_orani = 20
            else:
                vergi_orani = 0
            
            sip_b_fiyat = float(item['price']) / (1 + vergi_orani/100)
            sip_miktar = item['quantity']
            sip_tutar = sip_b_fiyat * sip_miktar
            sip_vergi = (sip_b_fiyat * vergi_orani/100) * sip_miktar

            # SQL sorgusunu hazırla
            sql = f"""INSERT INTO SIPARISLER (
                sip_Guid,sip_DBCno,sip_SpecRECno,sip_iptal,sip_fileid,sip_hidden,sip_kilitli,sip_degisti,sip_checksum,sip_create_user,
                sip_create_date,sip_lastup_user,sip_lastup_date,sip_special1,sip_special2,sip_special3,sip_firmano,sip_subeno,sip_tarih,
                sip_teslim_tarih,sip_tip,sip_cins,sip_evrakno_seri,sip_evrakno_sira,sip_satirno,sip_belgeno,sip_belge_tarih,sip_satici_kod,
                sip_musteri_kod,sip_stok_kod,sip_b_fiyat,sip_miktar,sip_birim_pntr,sip_teslim_miktar,sip_tutar,sip_iskonto_1,sip_iskonto_2,
                sip_iskonto_3,sip_iskonto_4,sip_iskonto_5,sip_iskonto_6,sip_masraf_1,sip_masraf_2,sip_masraf_3,sip_masraf_4,sip_vergi_pntr,sip_vergi,
                sip_masvergi_pntr,sip_masvergi,sip_opno,sip_aciklama,sip_aciklama2,sip_depono,sip_OnaylayanKulNo,sip_vergisiz_fl,sip_kapat_fl,sip_promosyon_fl,
                sip_cari_sormerk,sip_stok_sormerk,sip_cari_grupno,sip_doviz_cinsi,sip_doviz_kuru,sip_alt_doviz_kuru,sip_adresno,sip_teslimturu,sip_cagrilabilir_fl,
                sip_iskonto1,sip_iskonto2,sip_iskonto3,sip_iskonto4,sip_iskonto5,sip_iskonto6,sip_masraf1,sip_masraf2,sip_masraf3,sip_masraf4,sip_isk1,
                sip_isk2,sip_isk3,sip_isk4,sip_isk5,sip_isk6,sip_mas1,sip_mas2,sip_mas3,sip_mas4,sip_Exp_Imp_Kodu,sip_kar_orani,sip_durumu,sip_planlananmiktar,
                sip_parti_kodu,sip_lot_no,sip_projekodu,sip_fiyat_liste_no,sip_Otv_Pntr,sip_Otv_Vergi,sip_otvtutari,sip_OtvVergisiz_Fl,sip_paket_kod,
                sip_harekettipi,sip_kapatmanedenkod,sip_onodeme_evrak_tip,sip_onodeme_evrak_seri,sip_onodeme_evrak_sira,
                sip_rezervasyon_miktari,sip_rezerveden_teslim_edilen,sip_HareketGrupKodu1,sip_HareketGrupKodu2,sip_HareketGrupKodu3,sip_Olcu1,sip_Olcu2,sip_Olcu3,sip_Olcu4,
                sip_Olcu5,sip_FormulMiktarNo,sip_FormulMiktar,sip_satis_fiyat_doviz_cinsi,sip_satis_fiyat_doviz_kuru,sip_eticaret_kanal_kodu,sip_Tevkifat_turu,
                sip_otv_tevkifat_turu,sip_otv_tevkifat_tutari
                ) 
                VALUES (
            NEWID(),0,0,0,21,0,0,0,0,6,{tarih},1,{tarih},N'',N'',N'',0,0,'{formatted_date_without_time}','{formatted_date_without_time}',0,0,'{sip_evrakno_seri}',{sip_evrakno_sira},{sip_satirno},
            '{sip_belgeno}','{formatted_date_without_time}',N'','{sip_musteri_kod}','{sip_stok_kod}',{sip_b_fiyat},{sip_miktar},1,0,{sip_tutar},0,0,0,0,0,0,0,0,0,0,8,{sip_vergi},0,0,0,N'',N'',1,0,0,0,0,N'10',N'10',0,0,1.000000000000,
            27.526400000000,1,N'01',1,0,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,N'',0,0,0,N'',0,N'',1,0,0,0,0,N'',0,N'',0,N'',0,0,0,N'',N'',N'',0,0,0,0,0,0,0,0,1.000000000000,N'',0,0,0
            )"""

            # Sorguyu çalıştır
            print(sql)
            cursor.execute(sql)
            sip_satirno += 1  # Bir sonraki ürün için satır numarasını artır
        conn.commit()
