// Magical Bitcoin Library
// Written in 2020 by
//     Alekos Filini <alekos.filini@gmail.com>
//
// Copyright (c) 2020 Magical Bitcoin
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! Descriptors DSL

#[doc(hidden)]
#[macro_export]
macro_rules! impl_top_level_sh {
    ( $descriptor_variant:ident, $( $minisc:tt )* ) => {
        $crate::fragment!($( $minisc )*)
            .map(|(minisc, keymap, networks)|($crate::miniscript::Descriptor::<$crate::miniscript::descriptor::DescriptorPublicKey>::$descriptor_variant(minisc), keymap, networks))
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! impl_top_level_pk {
    ( $descriptor_variant:ident, $ctx:ty, $key:expr ) => {{
        #[allow(unused_imports)]
        use $crate::keys::{DescriptorKey, ToDescriptorKey};

        $key.to_descriptor_key()
            .and_then(|key: DescriptorKey<$ctx>| key.extract())
            .map(|(pk, key_map, valid_networks)| {
                (
                    $crate::miniscript::Descriptor::<
                        $crate::miniscript::descriptor::DescriptorPublicKey,
                    >::$descriptor_variant(pk),
                    key_map,
                    valid_networks,
                )
            })
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! impl_modifier {
    ( $terminal_variant:ident, $( $inner:tt )* ) => {
        $crate::fragment!($( $inner )*)
            .map_err(|e| -> $crate::Error { e.into() })
            .and_then(|(minisc, keymap, networks)| Ok(($crate::miniscript::Miniscript::from_ast($crate::miniscript::miniscript::decode::Terminal::$terminal_variant(std::sync::Arc::new(minisc)))?, keymap, networks)))
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! impl_leaf_opcode {
    ( $terminal_variant:ident ) => {
        $crate::miniscript::Miniscript::from_ast(
            $crate::miniscript::miniscript::decode::Terminal::$terminal_variant,
        )
        .map_err($crate::Error::Miniscript)
        .map(|minisc| {
            (
                minisc,
                $crate::miniscript::descriptor::KeyMap::default(),
                $crate::keys::any_network(),
            )
        })
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! impl_leaf_opcode_value {
    ( $terminal_variant:ident, $value:expr ) => {
        $crate::miniscript::Miniscript::from_ast(
            $crate::miniscript::miniscript::decode::Terminal::$terminal_variant($value),
        )
        .map_err($crate::Error::Miniscript)
        .map(|minisc| {
            (
                minisc,
                $crate::miniscript::descriptor::KeyMap::default(),
                $crate::keys::any_network(),
            )
        })
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! impl_leaf_opcode_value_two {
    ( $terminal_variant:ident, $one:expr, $two:expr ) => {
        $crate::miniscript::Miniscript::from_ast(
            $crate::miniscript::miniscript::decode::Terminal::$terminal_variant($one, $two),
        )
        .map_err($crate::Error::Miniscript)
        .map(|minisc| {
            (
                minisc,
                $crate::miniscript::descriptor::KeyMap::default(),
                $crate::keys::any_network(),
            )
        })
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! impl_node_opcode_two {
    ( $terminal_variant:ident, ( $( $a:tt )* ), ( $( $b:tt )* ) ) => {
        $crate::fragment!($( $a )*)
            .and_then(|a| Ok((a, $crate::fragment!($( $b )*)?)))
            .and_then(|((a_minisc, mut a_keymap, a_networks), (b_minisc, b_keymap, b_networks))| {
                // join key_maps
                a_keymap.extend(b_keymap.into_iter());

                Ok(($crate::miniscript::Miniscript::from_ast($crate::miniscript::miniscript::decode::Terminal::$terminal_variant(
                    std::sync::Arc::new(a_minisc),
                    std::sync::Arc::new(b_minisc),
                ))?, a_keymap, $crate::keys::merge_networks(&a_networks, &b_networks)))
            })
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! impl_node_opcode_three {
    ( $terminal_variant:ident, ( $( $a:tt )* ), ( $( $b:tt )* ), ( $( $c:tt )* ) ) => {
        $crate::fragment!($( $a )*)
            .and_then(|a| Ok((a, $crate::fragment!($( $b )*)?, $crate::fragment!($( $c )*)?)))
            .and_then(|((a_minisc, mut a_keymap, a_networks), (b_minisc, b_keymap, b_networks), (c_minisc, c_keymap, c_networks))| {
                // join key_maps
                a_keymap.extend(b_keymap.into_iter());
                a_keymap.extend(c_keymap.into_iter());

                let networks = $crate::keys::merge_networks(&a_networks, &b_networks);
                let networks = $crate::keys::merge_networks(&networks, &c_networks);

                Ok(($crate::miniscript::Miniscript::from_ast($crate::miniscript::miniscript::decode::Terminal::$terminal_variant(
                    std::sync::Arc::new(a_minisc),
                    std::sync::Arc::new(b_minisc),
                    std::sync::Arc::new(c_minisc),
                ))?, a_keymap, networks))
            })
    };
}

/// Macro to write full descriptors with code
///
/// This macro expands to an object of type `Result<(Descriptor<DescriptorPublicKey>, KeyMap, ValidNetworks), Error>`.
///
/// ## Example
///
/// Signature plus timelock, equivalent to: `sh(wsh(and_v(v:pk(...), older(...))))`
///
/// ```
/// # use std::str::FromStr;
/// let my_key = bitcoin::PublicKey::from_str("02e96fe52ef0e22d2f131dd425ce1893073a3c6ad20e8cac36726393dfb4856a4c")?;
/// let my_timelock = 50;
/// let (my_descriptor, my_keys_map, networks) = bdk::descriptor!(sh ( wsh ( and_v (+v pk my_key), ( older my_timelock ))))?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// -------
///
/// 2-of-3 that becomes a 1-of-3 after a timelock has expired. Both `descriptor_a` and `descriptor_b` are equivalent: the first
/// syntax is more suitable for a fixed number of items known at compile time, while the other accepts a
/// [`Vec`] of items, which makes it more suitable for writing dynamic descriptors.
///
/// They both produce the descriptor: `wsh(thresh(2,pk(...),s:pk(...),sdv:older(...)))`
///
/// ```
/// # use std::str::FromStr;
/// let my_key_1 = bitcoin::PublicKey::from_str("02e96fe52ef0e22d2f131dd425ce1893073a3c6ad20e8cac36726393dfb4856a4c")?;
/// let my_key_2 = bitcoin::PrivateKey::from_wif("cVt4o7BGAig1UXywgGSmARhxMdzP5qvQsxKkSsc1XEkw3tDTQFpy")?;
/// let my_timelock = 50;
///
/// let (descriptor_a, key_map_a, networks) = bdk::descriptor! {
///     wsh (
///         thresh 2, (pk my_key_1), (+s pk my_key_2), (+s+d+v older my_timelock)
///     )
/// }?;
///
/// let b_items = vec![
///     bdk::fragment!(pk my_key_1)?,
///     bdk::fragment!(+s pk my_key_2)?,
///     bdk::fragment!(+s+d+v older my_timelock)?,
/// ];
/// let (descriptor_b, mut key_map_b, networks) = bdk::descriptor!( wsh ( thresh_vec 2, b_items ) )?;
///
/// assert_eq!(descriptor_a, descriptor_b);
/// assert_eq!(key_map_a.len(), key_map_b.len());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// ------
///
/// Simple 2-of-2 multi-signature, equivalent to: `wsh(multi(2, ...))`
///
/// ```
/// # use std::str::FromStr;
/// let my_key_1 = bitcoin::PublicKey::from_str("02e96fe52ef0e22d2f131dd425ce1893073a3c6ad20e8cac36726393dfb4856a4c")?;
/// let my_key_2 = bitcoin::PrivateKey::from_wif("cVt4o7BGAig1UXywgGSmARhxMdzP5qvQsxKkSsc1XEkw3tDTQFpy")?;
///
/// let (descriptor, key_map, networks) = bdk::descriptor! {
///     wsh (
///         multi 2, my_key_1, my_key_2
///     )
/// }?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// ------
///
/// Native-Segwit single-sig, equivalent to: `wpkh(...)`
///
/// ```
/// let my_key = bitcoin::PrivateKey::from_wif("cVt4o7BGAig1UXywgGSmARhxMdzP5qvQsxKkSsc1XEkw3tDTQFpy")?;
///
/// let (descriptor, key_map, networks) = bdk::descriptor!(wpkh ( my_key ) )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[macro_export]
macro_rules! descriptor {
    ( bare ( $( $minisc:tt )* ) ) => ({
        $crate::impl_top_level_sh!(Bare, $( $minisc )*)
    });
    ( sh ( wsh ( $( $minisc:tt )* ) ) ) => ({
        $crate::descriptor!(shwsh ($( $minisc )*))
    });
    ( shwsh ( $( $minisc:tt )* ) ) => ({
        $crate::impl_top_level_sh!(ShWsh, $( $minisc )*)
    });
    ( pk $key:expr ) => ({
        $crate::impl_top_level_pk!(Pk, $crate::miniscript::Legacy, $key)
    });
    ( pkh $key:expr ) => ({
        $crate::impl_top_level_pk!(Pkh,$crate::miniscript::Legacy, $key)
    });
    ( wpkh $key:expr ) => ({
        $crate::impl_top_level_pk!(Wpkh, $crate::miniscript::Segwitv0, $key)
    });
    ( sh ( wpkh ( $key:expr ) ) ) => ({
        $crate::descriptor!(shwpkh ( $key ))
    });
    ( shwpkh ( $key:expr ) ) => ({
        $crate::impl_top_level_pk!(ShWpkh, $crate::miniscript::Segwitv0, $key)
    });
    ( sh ( $( $minisc:tt )* ) ) => ({
        $crate::impl_top_level_sh!(Sh, $( $minisc )*)
    });
    ( wsh ( $( $minisc:tt )* ) ) => ({
        $crate::impl_top_level_sh!(Wsh, $( $minisc )*)
    });
}

/// Macro to write descriptor fragments with code
///
/// This macro will be expanded to an object of type `Result<(Miniscript<DescriptorPublicKey, _>, KeyMap, ValidNetworks), Error>`. It allows writing
/// fragments of larger descriptors that can be pieced together using `fragment!(thresh_vec ...)`.
#[macro_export]
macro_rules! fragment {
    // Modifiers
    ( +a $( $inner:tt )* ) => ({
        $crate::impl_modifier!(Alt, $( $inner )*)
    });
    ( +s $( $inner:tt )* ) => ({
        $crate::impl_modifier!(Swap, $( $inner )*)
    });
    ( +c $( $inner:tt )* ) => ({
        $crate::impl_modifier!(Check, $( $inner )*)
    });
    ( +d $( $inner:tt )* ) => ({
        $crate::impl_modifier!(DupIf, $( $inner )*)
    });
    ( +v $( $inner:tt )* ) => ({
        $crate::impl_modifier!(Verify, $( $inner )*)
    });
    ( +j $( $inner:tt )* ) => ({
        $crate::impl_modifier!(NonZero, $( $inner )*)
    });
    ( +n $( $inner:tt )* ) => ({
        $crate::impl_modifier!(ZeroNotEqual, $( $inner )*)
    });
    ( +t $( $inner:tt )* ) => ({
        $crate::fragment!(and_v ( $( $inner )* ), ( true ) )
    });
    ( +l $( $inner:tt )* ) => ({
        $crate::fragment!(or_i ( false ), ( $( $inner )* ) )
    });
    ( +u $( $inner:tt )* ) => ({
        $crate::fragment!(or_i ( $( $inner )* ), ( false ) )
    });

    // Miniscript
    ( true ) => ({
        $crate::impl_leaf_opcode!(True)
    });
    ( false ) => ({
        $crate::impl_leaf_opcode!(False)
    });
    ( pk_k $key:expr ) => ({
        $crate::keys::make_pk($key)
    });
    ( pk $key:expr ) => ({
        $crate::fragment!(+c pk_k $key)
    });
    ( pk_h $key_hash:expr ) => ({
        $crate::impl_leaf_opcode_value!(PkH, $key_hash)
    });
    ( after $value:expr ) => ({
        $crate::impl_leaf_opcode_value!(After, $value)
    });
    ( older $value:expr ) => ({
        $crate::impl_leaf_opcode_value!(Older, $value)
    });
    ( sha256 $hash:expr ) => ({
        $crate::impl_leaf_opcode_value!(Sha256, $hash)
    });
    ( hash256 $hash:expr ) => ({
        $crate::impl_leaf_opcode_value!(Hash256, $hash)
    });
    ( ripemd160 $hash:expr ) => ({
        $crate::impl_leaf_opcode_value!(Ripemd160, $hash)
    });
    ( hash160 $hash:expr ) => ({
        $crate::impl_leaf_opcode_value!(Hash160, $hash)
    });
    ( and_v ( $( $a:tt )* ), ( $( $b:tt )* ) ) => ({
        $crate::impl_node_opcode_two!(AndV, ( $( $a )* ), ( $( $b )* ))
    });
    ( and_b ( $( $a:tt )* ), ( $( $b:tt )* ) ) => ({
        $crate::impl_node_opcode_two!(AndB, ( $( $a )* ), ( $( $b )* ))
    });
    ( and_or ( $( $a:tt )* ), ( $( $b:tt )* ), ( $( $c:tt )* ) ) => ({
        $crate::impl_node_opcode_three!(AndOr, ( $( $a )* ), ( $( $b )* ), ( $( $c )* ))
    });
    ( or_b ( $( $a:tt )* ), ( $( $b:tt )* ) ) => ({
        $crate::impl_node_opcode_two!(OrB, ( $( $a )* ), ( $( $b )* ))
    });
    ( or_d ( $( $a:tt )* ), ( $( $b:tt )* ) ) => ({
        $crate::impl_node_opcode_two!(OrD, ( $( $a )* ), ( $( $b )* ))
    });
    ( or_c ( $( $a:tt )* ), ( $( $b:tt )* ) ) => ({
        $crate::impl_node_opcode_two!(OrC, ( $( $a )* ), ( $( $b )* ))
    });
    ( or_i ( $( $a:tt )* ), ( $( $b:tt )* ) ) => ({
        $crate::impl_node_opcode_two!(OrI, ( $( $a )* ), ( $( $b )* ))
    });
    ( thresh_vec $thresh:expr, $items:expr ) => ({
        use $crate::miniscript::descriptor::KeyMap;

        let (items, key_maps_networks): (Vec<_>, Vec<_>) = $items.into_iter().map(|(a, b, c)| (a, (b, c))).unzip();
        let items = items.into_iter().map(std::sync::Arc::new).collect();

        let (key_maps, valid_networks) = key_maps_networks.into_iter().fold((KeyMap::default(), $crate::keys::any_network()), |(mut keys_acc, net_acc), (key, net)| {
            keys_acc.extend(key.into_iter());
            let net_acc = $crate::keys::merge_networks(&net_acc, &net);

            (keys_acc, net_acc)
        });

        $crate::impl_leaf_opcode_value_two!(Thresh, $thresh, items)
            .map(|(minisc, _, _)| (minisc, key_maps, valid_networks))
    });
    ( thresh $thresh:expr $(, ( $( $item:tt )* ) )+ ) => ({
        let mut items = vec![];
        $(
            items.push($crate::fragment!($( $item )*));
        )*

        items.into_iter().collect::<Result<Vec<_>, _>>()
            .and_then(|items| $crate::fragment!(thresh_vec $thresh, items))
    });
    ( multi_vec $thresh:expr, $keys:expr ) => ({
        $crate::keys::make_multi($thresh, $keys)
    });
    ( multi $thresh:expr $(, $key:expr )+ ) => ({
        use $crate::keys::ToDescriptorKey;

        let mut keys = vec![];
        $(
            keys.push($key.to_descriptor_key());
        )*

        keys.into_iter().collect::<Result<Vec<_>, _>>()
            .and_then(|keys| $crate::keys::make_multi($thresh, keys))
    });

}
