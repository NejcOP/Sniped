#!/usr/bin/env python3
"""Check SQLite contents"""
import sqlite3
from pathlib import Path

db_path = Path("leads.db")
if not db_path.exists():
    print("SQLite datoteka ne obstaja!")
    exit(0)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Preberi vse tabele
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()

print("TABELE V SQLite:")
print("=" * 60)
for table in tables:
    table_name = table[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"  • {table_name}: {count} zapisa")

conn.close()

print("\n" + "=" * 60)
print("✅ VARNO JE ZBRISATI - Zakaj?")
print("=" * 60)
print("\n1. Svi CRM podatki so ŽE V SUPABASE")
print("2. Backend je v PRIMARY MODE - bere samo iz Supabase")
print("3. SQLite je SAMO fallback (if Supabase fails)")
print("\nKaj da naredim?")
print("━" * 60)
print("OPCIJA A: Naredi BACKUP (varno)")
print("  → leads.db.backup → ohrani varnost")
print("\nOPCIJA B: Briši - ampak keep fallback kodo v backendu")
print("  → Supabase je glavni, SQLite kot emergency backup")
print("\nOPCIJA C: Briši popolnoma - no fallback")
print("  → Čist sistem, ampak brez fallback-a")
print("\nPriporočam OPCIJO A ali B!")
