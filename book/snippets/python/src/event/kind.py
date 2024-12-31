from typing import cast
from nostr_sdk import Kind, KindEnum, EventBuilder, Keys, Metadata


def kind():
    print()
    keys = Keys.generate()
    print("Kind:")

    # ANCHOR: kind-int
    print("  Kind from integer:")
    kind = Kind(1)
    print(f"     - Kind 1: {kind.as_enum()}")
    kind = Kind(0)
    print(f"     - Kind 0: {kind.as_enum()}")
    kind = Kind(3)
    print(f"     - Kind 3: {kind.as_enum()}")
    # ANCHOR_END: kind-int

    print()
    # ANCHOR: kind-enum
    print("  Kind from enum:")
    kind = Kind.from_enum(cast(KindEnum, KindEnum.TEXT_NOTE()))
    print(f"     - Kind TEXT_NOTE: {kind.as_u16()}")
    kind = Kind.from_enum(cast(KindEnum, KindEnum.METADATA()))
    print(f"     - Kind METADATA: {kind.as_u16()}")
    kind = Kind.from_enum(cast(KindEnum, KindEnum.CONTACT_LIST()))
    print(f"     - Kind CONTRACT_LIST: {kind.as_u16()}")
    # ANCHOR_END: kind-enum

    print()
    # ANCHOR: kind-methods
    print("  Kind methods EventBuilder:")
    event  = EventBuilder.text_note("This is a note").sign_with_keys(keys)
    print(f"     - Kind text_note(): {event.kind().as_u16()} - {event.kind().as_enum()}")
    event  = EventBuilder.metadata(Metadata()).sign_with_keys(keys)
    print(f"     - Kind metadata(): {event.kind().as_u16()} - {event.kind().as_enum()}")
    event  = EventBuilder.contact_list([]).sign_with_keys(keys)
    print(f"     - Kind contact_list(): {event.kind().as_u16()} - {event.kind().as_enum()}")
    # ANCHOR_END: kind-methods

    print()
    # ANCHOR: kind-representations
    kind = Kind(1337)
    print(f"Custom Event Kind: {kind.as_u16()} - {kind.as_enum()}")
    # ANCHOR_END: kind-representations
